# About Hex.pm Releases

Releases of [esqlite on hex.pm](https://hex.pm/packages/esqlite) are managed by Connor Rigby ([@connorrigby](https://github.com/connorrigby)).

## Setup
1. Install rebar3
2. [Install hex plugin](https://hex.pm/docs/rebar3_usage#installation)
3. Run `rebar3 update`.

## Make a Release

1. Increment the version number in `src/esqlite.app.src` as appropriate.
2. Run `rebar3 ct` to ensure that the build is good.
3. **IMPORTANT:** Run `rebar3 clean` to remove build artifacts (especially compiled C object code).
4. Run `rebar3 hex publish` to make the release.
