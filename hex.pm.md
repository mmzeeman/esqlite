# About Hex.pm Releases

Releases of [esqlite on hex.pm](https://hex.pm/packages/esqlite) are managed by Connor Rigby ([@connorrigby](https://github.com/connorrigby)).

To make a release:

1. Increment the version number in `src/esqlite.app.src` as appropriate.
2. Run `rebar3 ct` to ensure that the build is good.
3. **IMPORTANT:** Run `rebar3 clean` to remove build artifacts (especially compiled C object code).
4. Run `rebar3 hex publish` to make the release.
