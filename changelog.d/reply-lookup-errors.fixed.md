- `send_message` reply destination lookup failures now return a fixed,
  sanitized error, including malformed JSON and transport failures. Client
  exception details, API codes and response bodies are never forwarded.
