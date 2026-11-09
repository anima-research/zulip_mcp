- Move-recovery errors only suggest an explicit stream `send_message` when
  both the stream and topic are non-empty; otherwise use the `reply_to` hint.
