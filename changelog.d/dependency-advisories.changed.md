- An image attachment with more than five channels is no longer inlined:
  sharp 0.35 refuses such input by default (`limitInputChannels`), so where
  it would have been inlined it now arrives as a "could not be inlined" note
  with its `fetch_attachment` path.
