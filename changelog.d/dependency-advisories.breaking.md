- **Hosts on Node 18, 19 or 20.0–20.9:** the server now requires Node 20.10 or
  later (`engines.node` is `>=20.10.0`; 2.2.1 allowed Node 18 and later).
  sharp 0.35's ESM build, which the server imports, reads its own
  `package.json` with `import … with { type: 'json' }`, which Node 20 parses
  from 20.10.0, though sharp itself declares `>=20.9.0`.
- **Hosts sharp 0.35's prebuilt binaries don't cover:** image inlining uses
  sharp 0.35. Its prebuilt Linux binaries need glibc 2.28 or later on x64
  and arm64 and glibc 2.36 or later on 32-bit ARM and s390x, and its musl
  binaries need musl 1.2.5 or later. On Linux x64, sharp checks for the
  x86-64-v2 instructions when it loads and refuses its binary on a CPU
  without them. Its 32-bit Windows binary declares Node 20 only
  (`^20.9.0`), and npm skips an optional package whose declared Node range
  doesn't match, so on 32-bit Windows with Node 21 or later it isn't
  installed. On such a host, or on a platform with no prebuilt binary,
  sharp loads only from its WebAssembly build or a manual source build.
- **Hosts installed with npm 10 where sharp's native binary can't load:**
  npm 10, which Node 20 from 20.10, 21, 22 and 23 ship, installs sharp's
  WebAssembly build alongside sharp 0.35 (observed on Linux x64), and npm
  11, which Node 24, 25 and 26 ship, doesn't, except on FreeBSD, where
  sharp's FreeBSD package depends on that build. Where that build is
  installed and the native binary can't load, sharp loads the WebAssembly
  build without saying so, and the server starts and inlines images on it,
  more slowly and with more memory.
- **Hosts where sharp can't load:** images are inlined by default, and the
  server loads sharp at start when images may be inlined:
  `ZULIP_INLINE_IMAGES` isn't `false`, and `ZULIP_INLINE_IMAGES_MAX` doesn't
  set the limit to 0. Where sharp can't load then, the server exits,
  printing sharp's error, that loading sharp needs Node 20.10 or later, and
  the two ways through: set `ZULIP_INLINE_IMAGES=false`, or install sharp's
  WebAssembly build where the server is installed, for which it prints the
  `npm install --no-save` command and where to run it. Otherwise the server
  doesn't load sharp. The command leaves `package.json` and
  `package-lock.json` unchanged, and with npm 11 a later `npm install` or
  `npm ci` removes the build again, except on FreeBSD.
