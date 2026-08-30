import { createHash } from "node:crypto";
import type { SupervisorEvent } from "./supervisor-contracts.js";
import { redactSupervisorEvent } from "./supervisor-redaction.js";
import type { SupervisorAlertInput, SupervisorAlertSeverity } from "./supervisor-ledger.js";

/**
 * Deterministic suspicious-activity rules.
 *
 * These rules classify; they never block. Keeping classification deterministic
 * is what makes it testable and what lets the operator chatbot explain an alert
 * from stored evidence instead of inventing a judgement of its own.
 */
export interface SuspiciousRule {
  id: string;
  title: string;
  severity: SupervisorAlertSeverity;
  /** Why this pattern matters, shown next to the evidence in the dashboard. */
  rationale: string;
  pattern: RegExp;
}

export interface SuspiciousMatch {
  rule: SuspiciousRule;
  /** The matched text plus surrounding context, already redacted. */
  evidence: string;
}

export const SUSPICIOUS_RULES: SuspiciousRule[] = [
  {
    id: "secret-file-access",
    title: "Attempt to read secrets or credential files",
    severity: "critical",
    rationale:
      "Agent workspaces never need these files; reading them is the first step of credential theft.",
    pattern:
      /(?:^|[\s"'`;|&(<])(?:cat|less|more|head|tail|bat|nl|xxd|od|strings|grep|rg|awk|sed)\b[^\n;|&]{0,120}?(?:\.env(?:\.[\w-]+)?|\.npmrc|\.git-credentials|\.aws\/credentials|\.ssh\/id_[\w]+|\.docker\/config\.json|\.kube\/config|\/etc\/shadow|\/etc\/passwd|credentials\.json|secrets?\.(?:ya?ml|json|txt))/i,
  },
  {
    id: "destructive-filesystem",
    title: "Destructive filesystem command",
    severity: "critical",
    rationale:
      "These commands destroy the workspace or the host disk rather than editing files.",
    pattern:
      /(?:rm\s+(?:-[a-z]*[rf][a-z]*\s+)+(?:\/(?:\s|$)|\/(?:etc|usr|var|home|root|bin|lib|workspace)\b|~(?:\/|\s|$)|\*)|mkfs(?:\.\w+)?\s|dd\s+[^\n]*of=\/dev\/(?:sd|nvme|vd|xvd)|shred\s+[^\n]*\/dev\/|>\s*\/dev\/(?:sd|nvme|vd)[a-z]|chmod\s+-R\s+777\s+\/(?:\s|$)|:\(\)\s*\{\s*:\|:&\s*\}\s*;\s*:)/i,
  },
  {
    id: "credential-exfiltration",
    title: "Possible credential or data exfiltration",
    severity: "critical",
    rationale:
      "Sending environment variables, key material, or archives to an outside host moves data off the machine.",
    pattern:
      /(?:(?:curl|wget)\b[^\n]{0,200}?(?:--data|--data-binary|--upload-file|-d\s|-F\s|-T\s)[^\n]{0,120}?(?:\$\(?env\)?|\.env\b|id_[\w]+\b|token|secret|password|credential)|(?:env|printenv|cat\s+\.env)\b[^\n]{0,60}\|\s*(?:curl|wget|nc|ncat|netcat|socat)\b|\b(?:nc|ncat|netcat)\b[^\n]{0,80}\b\d{1,3}(?:\.\d{1,3}){3}\b[^\n]{0,20}\d{2,5}|\bscp\s+[^\n]{0,120}@[^\s:]+:|https?:\/\/(?:[\w.-]*\.)?(?:pastebin\.com|transfer\.sh|file\.io|0x0\.st|webhook\.site|requestbin\.[\w.]+|ngrok\.io|burpcollaborator\.net)\b)/i,
  },
  {
    id: "privilege-escalation",
    title: "Privilege escalation or container escape attempt",
    severity: "critical",
    rationale:
      "The Runtime is deliberately unprivileged; reaching for the Docker socket or the host namespace breaks the sandbox boundary.",
    pattern:
      /(?:\/var\/run\/docker\.sock|\bdocker\s+(?:run|create|exec)\b[^\n]{0,160}(?:--privileged|--pid[= ]host|--net(?:work)?[= ]host|-v\s*\/:\/|--cap-add[= ]?(?:SYS_ADMIN|ALL))|\bnsenter\b|\bchroot\s+\/(?:host|mnt|proc)|\bsudo\s+(?:su|-i|-s|bash|sh)\b|\bsu\s+-\s*root\b|setcap\s+cap_|\bmount\b[^\n]{0,80}\/proc\/\d+\/root|\/proc\/self\/(?:exe|environ)\b|\bkubectl\s+exec\b)/i,
  },
  {
    id: "unexpected-package-execution",
    title: "Remote script piped into a shell",
    severity: "warning",
    rationale:
      "Downloading and executing an unreviewed remote script runs code that never appears in the workspace history.",
    pattern:
      /(?:curl|wget)\b[^\n|]{0,200}\|\s*(?:sudo\s+)?(?:ba|z|k|d)?sh\b/i,
  },
];

const MAX_EVIDENCE = 400;

/** The fields a rule is allowed to see, joined into one searchable haystack. */
function haystack(event: SupervisorEvent): string {
  const parts: string[] = [event.summary];
  const payload = event.payload;
  for (const key of ["command", "detail", "reason", "output"] as const) {
    const value = payload[key];
    if (typeof value === "string") parts.push(value);
  }
  return parts.join("\n");
}

function excerpt(source: string, index: number, length: number): string {
  const start = Math.max(0, index - 60);
  const end = Math.min(source.length, index + length + 60);
  const prefix = start > 0 ? "…" : "";
  const suffix = end < source.length ? "…" : "";
  return (
    prefix +
    source.slice(start, end).replace(/\s+/g, " ").trim() +
    suffix
  ).slice(0, MAX_EVIDENCE);
}

/**
 * Evaluates one event against every rule. The event is redacted first, so alert
 * evidence can never reintroduce a secret that redaction removed elsewhere.
 */
export function evaluateSuspiciousActivity(
  event: SupervisorEvent,
  rules: SuspiciousRule[] = SUSPICIOUS_RULES,
): SuspiciousMatch[] {
  const sanitized = redactSupervisorEvent(event);
  const source = haystack(sanitized);
  if (!source.trim()) return [];
  const matches: SuspiciousMatch[] = [];
  for (const rule of rules) {
    const match = rule.pattern.exec(source);
    if (!match) continue;
    matches.push({
      rule,
      evidence: excerpt(source, match.index, match[0].length),
    });
  }
  return matches;
}

/**
 * Alert IDs are a hash of rule + run + evidence.
 *
 * Keying on the event alone would be replay-safe but operationally noisy: Codex
 * reports a command both at start and at completion, and an Agent often repeats
 * the same command, so one behaviour would raise four near-identical alerts.
 * Keying on the evidence collapses them into a single alert that counts its
 * occurrences and points at the event that triggered it first.
 */
export function toAlertInput(
  event: SupervisorEvent,
  match: SuspiciousMatch,
): SupervisorAlertInput {
  const alertId = createHash("sha256")
    .update(match.rule.id + " " + event.runId + " " + match.evidence)
    .digest("hex")
    .slice(0, 32);
  return {
    alertId,
    runId: event.runId,
    eventId: event.eventId,
    ruleId: match.rule.id,
    severity: match.rule.severity,
    evidence: match.evidence,
    createdAt: event.occurredAt,
  };
}
