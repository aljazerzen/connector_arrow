# Developer's Guide

## Development environment

This project specifies development environment using a [nix shell](./flake.nix).

To load the shell:

1. [Install nix (the package manager)](https://nixos.org/download). (only first
   time)

2. Enable flakes, which are a (pretty stable) experimental feature of nix. (only
   first time)

   For non-NixOS users:

   ```sh
   mkdir -p ~/.config/nix/
   tee 'experimental-features = nix-command flakes' >> ~/.config/nix/nix.conf
   ```

   For NixOs users, follow instructions [here](https://nixos.wiki/wiki/Flakes).

3. Run:

   ```sh
   nix develop
   ```

Optionally, you can install [direnv](https://direnv.net/), to automatically load
the shell when you enter this repo. The easiest way is to also install
[direnv-nix](https://github.com/nix-community/nix-direnv) and configure your
`.envrc` with:

```sh
# .envrc
use flake .#full
```

Additionally, Docker is required for running the test databases.

## Running and testing

This project specifies commands for running and testing using [Just](https://just.systems/man/en/).

To run the full test suite, run:

```
just dbs/start
just dbs/seed
just test
```

See [Justfile](./Justfile) for all commands available.
