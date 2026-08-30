import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, setAuthToken } from "./api";

const fetchMock = vi.fn();

function mockResponse(
  body: unknown,
  options: { ok?: boolean; status?: number } = {},
): void {
  fetchMock.mockResolvedValueOnce({
    ok: options.ok ?? true,
    status: options.status ?? 200,
    json: async () => body,
  } as Response);
}

describe("supervisor API client", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", fetchMock);
    setAuthToken("");
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("sends an authenticated cancellation with a reason", async () => {
    const runId = "10000000-0000-4000-8000-000000000001";
    setAuthToken("  local-test-token  ");
    mockResponse({
      commandId: "20000000-0000-4000-8000-000000000002",
      runId,
      published: true,
    });

    await expect(api.supervisorCancel(runId, "Operator test")).resolves.toMatchObject({
      runId,
      published: true,
    });

    const [url, request] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/api/supervisor/runs/" + runId + "/cancel");
    expect(request.method).toBe("POST");
    expect(request.headers).toMatchObject({
      Authorization: "Bearer local-test-token",
      "Content-Type": "application/json",
    });
    expect(JSON.parse(String(request.body))).toEqual({ reason: "Operator test" });
  });

  it("includes the selected run when asking the operator chatbot", async () => {
    const runId = "30000000-0000-4000-8000-000000000003";
    mockResponse({ answer: "Stored evidence.", citations: [], toolCalls: [] });

    await api.supervisorChat("What happened?", runId);

    const [url, request] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/api/supervisor/chat");
    expect(request.method).toBe("POST");
    expect(JSON.parse(String(request.body))).toEqual({
      question: "What happened?",
      runId,
    });
  });

  it("surfaces the server error and HTTP status", async () => {
    mockResponse(
      { error: "This run has already reached a terminal state" },
      { ok: false, status: 409 },
    );

    const result = api.supervisorCancel(
      "40000000-0000-4000-8000-000000000004",
    );
    await expect(result).rejects.toEqual(
      expect.objectContaining({
        message: "This run has already reached a terminal state",
        status: 409,
      }),
    );
  });
});
