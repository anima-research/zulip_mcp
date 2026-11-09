- Recognize verified Zulip quotes of the connector's own messages as addressed
  replies with `chat:reply`, distinct from explicit `chat:mention`.
- Push origins and channel-message metadata expose umbrella `isMention`,
  plus `isExplicitMention` and `isReplyToBot` flags for gate matching.
