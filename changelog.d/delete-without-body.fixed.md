- A request with no parameters, such as `delete_message`'s, is sent with no body.
  Node 20.10.0's `fetch` refused the empty body it used to carry, so
  `delete_message` failed there.
