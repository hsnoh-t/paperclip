export const DONE_COMMENT_REOPEN_DUPLICATE_WINDOW_MS = 60 * 60 * 1000;

export function normalizeIssueCommentBodyForReopenDedupe(body: string) {
  return body.replace(/\r\n?/g, "\n").trim().replace(/\s+/g, " ");
}
