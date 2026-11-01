- `edit_message` sends the message ID only in the URL path and the content in
  the PATCH body, and reports unsupported-parameter warnings as errors instead
  of success.
- `edit_message` rejects invalid message IDs and missing or non-string content
  before sending a request, and preserves HTTP error status and diagnostics for
  HTML or empty responses.
