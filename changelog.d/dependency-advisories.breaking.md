- **Hosts on Node 20.0–20.9:** the server now requires Node 20.10 or later
  (`engines.node` is `>=20.10.0`). sharp 0.35's ESM build, which the server
  imports, reads its own `package.json` with `import … with { type: 'json' }`,
  which Node 20 parses from 20.10.0, though sharp itself declares `>=20.9.0`.
- **Hosts that relied on sharp's install-time source build:** sharp 0.33.5's
  install script built sharp from source when it found a globally installed
  libvips, or when `npm_config_build_from_source` was set, with
  node-addon-api and node-gyp present. sharp 0.35 removed that script, so it
  no longer builds against a global libvips when it installs. Without a
  manual source build, it selects an available published native or
  WebAssembly binary, or fails to load. On a platform its prebuilt binaries
  don't cover, such a host now needs sharp's WebAssembly build or a manual
  source build.
- **Hosts sharp 0.35's prebuilt binaries no longer cover:** the glibc
  sharp's prebuilt Linux binaries require rises from 2.26 to 2.28 on x64 and
  arm64, from 2.28 to 2.36 on 32-bit ARM, and from 2.31 to 2.36 on s390x,
  and the musl they require rises from 1.2.2 to 1.2.5. On Linux x64 they
  also require a CPU with the x86-64-v2 instructions, which sharp checks
  when it loads. Its 32-bit Windows binary is deprecated and now declares
  Node 20 only (`^20.9.0`), where 0.33.5's also allowed Node 21 and later,
  and npm skips an optional package whose declared Node range doesn't match,
  so on 32-bit Windows with Node 21 or later it isn't installed. On such a
  host, unless sharp's WebAssembly build is installed, sharp fails to load.
- **Where sharp can't load,** the server now says so at start. When images
  may be inlined, as by default (`ZULIP_INLINE_IMAGES` isn't `false` and
  `ZULIP_INLINE_IMAGES_MAX` is above 0), it exits, printing sharp's error,
  that loading sharp needs Node 20.10 or later, and the two ways through:
  set `ZULIP_INLINE_IMAGES=false`, or install sharp's WebAssembly build
  where the server is installed, for which it prints the versioned `npm
  install` command and the directory to run it in. Otherwise the server
  doesn't load sharp.
