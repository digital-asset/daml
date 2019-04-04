# Hazel Configuration

The file `packages.bzl` in this directory lists all packages available in the
chosen Stackage snapshot.

## Update the Stackage snapshot

To update the Stackage snapshot you need to clone the hazel repository into a
separate directory:

```
$ git clone https://github.com/FormationAI/hazel.git
```

Change into the hazel repository root directory.

```
$ cd hazel
```

Then execute the following command to update to the specified Stackage
snapshot, where `$PROJECT` points to the root of this repository:
(Requires `stack`)

```
$ Stackage.hs lts-12.4 "$PROJECT/hazel/packages.bzl"
```

On NixOS you may need to modify `Stackage.hs` to append the following flag to
the list of `stack` intrepreter flags: `--nix-packages zlib`.

This will take a while.
