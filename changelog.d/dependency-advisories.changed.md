- An image attachment with more than five channels is no longer inlined:
  sharp 0.35 refuses such input by default (`limitInputChannels`), so where
  it would have been inlined it now arrives as a "could not be inlined" note
  with its `fetch_attachment` path.
- On Node 20 before 20.18.3, a start where images may be inlined now prints
  Node's warning that importing JSON modules is experimental. It comes from
  sharp 0.35 and is harmless.
