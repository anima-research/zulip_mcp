- `MCPL_CONTEXT_POSITION=afterUser` declares the `zulip.context` injection
  ("Recent messages from …") at `afterUser` in place of `beforeUser`. The
  manifest then asks for `contextHooks.beforeInference.inject.afterUser`
  alone, so the host has to grant that path: one that grants only
  `beforeUser` gets no injection, and its `featureSets/update` receipt names
  `zulip.context` as unavailable. Unset, nothing changes. A value that names
  neither position is ignored at start, with a line saying so. What a host
  does with each position is the host's to say: agent-framework places a
  `beforeUser` injection afresh at every request, and stores an `afterUser`
  one in the agent's history each time its text changes (#47).
