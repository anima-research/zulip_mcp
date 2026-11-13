- An image attachment with more than five channels isn't inlined: sharp
  0.35 refuses such input by default (`limitInputChannels`), so it arrives
  as a "could not be inlined" note with its `fetch_attachment` path.
- A start where images may be inlined now prints Node's warning that
  importing JSON modules is experimental. That happens on Node 20 before
  20.18.3, Node 21, Node 22 before 22.12.0 and Node 23.0.0. The warning
  comes from sharp 0.35 and is harmless.
