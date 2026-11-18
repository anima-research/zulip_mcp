- Messages the server held when it stopped (refused by the host, lost with
  their offer, or still waiting at an orderly stop) are saved by id in
  `<session>.delivery.json`, 50 per channel. Until now a restart forgot
  them: if the next start's catch-up failed or stopped short of one and the
  host then accepted something newer, the watermark passed it and it was
  never offered. Such an id is now read by id at the following start and
  offered once if selected, in a `<missed>` block of its own before the
  catch-up (as messages on an open channel). Closed-stream ambient rows
  outside the addressed-message vicinity are withdrawn without an offer. The watermark moves as before, and that
  offer never moves it. Past 50 the oldest ids leave the list, whichever
  run held them, with a line on stderr giving their id range; a read by id that
  fails three starts running drops its id with one. A message the server
  never held is not on the list.
