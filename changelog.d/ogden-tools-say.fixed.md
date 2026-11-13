- When `send_message` or `send_dm` fails after posting part of a long
  message, the error names the posted parts by id and says that sending the
  whole text again posts them twice, before Zulip's words or the failure's
  own.
- A long line with no line end is no longer cut between the two halves of
  one character, such as an emoji, so neither message gets a replacement
  character.
- While the filters file is invalid or missing, a filters tool's refusal
  says that the file is the operator's to repair, and that an update can be
  made again once it is repaired.
