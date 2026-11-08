- Token and `send_message` reply destination lookups share sanitized errors:
  `BAD_REQUEST` means not found/not accessible; rate limits, server errors and
  transport/parse failures retain a safe code and advise retrying. API bodies
  and raw client exception details are never forwarded.
