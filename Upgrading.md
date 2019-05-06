# How to upgrade packages in this repo

## Stackage

To update the Stackage snapshot you need to clone the hazel repository into a
separate directory:

```
$ git clone https://github.com/tweag/rules_haskell.git
```

Change into the hazel directory.

```
$ cd rules_haskell/hazel
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


## Nixpkgs

TODO
