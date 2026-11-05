- `send_message` and `edit_message` insert missing top-level pipe-table boundary
  blanks after message-link resolution, only in simple messages. Existing bytes,
  LF/CRLF, edges and fenced content are preserved; normalization is idempotent.
  Zulip 12.3's real `markdown_convert` now verifies the rendering differential.
  Remote lazy-list prefixes, headings/rules and trailing prose whitespace in
  the whole table paragraph refuse the message. Indented/unsupported fence
  markers refuse too: Zulip requires column-zero exact-length closers.
  Paragraph safety is computed once and table-column results are cached,
  eliminating quadratic rescans. Python is test-only, never used at runtime.
  Agents should still supply table-boundary blanks themselves in complex Markdown.
