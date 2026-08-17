# -- ghc bindist --
# https://downloads.haskell.org/~ghc/
GHC_VERSION = "9.0.2"

GHC_BINDISTS = {
    ("linux", "amd64"): {
        "triple": "x86_64-deb10-linux",
        "sha256": "5d0b9414b10cfb918453bcd01c5ea7a1824fe95948b08498d6780f20ba247afc",
        "strip_prefix": "ghc-{}".format(GHC_VERSION),
    },
    ("darwin", "aarch64"): {
        "triple": "aarch64-apple-darwin",
        "sha256": "b1fcab17fe48326d2ff302d70c12bc4cf4d570dfbbce68ab57c719cfec882b05",
        "strip_prefix": "ghc-{}-aarch64-apple-darwin".format(GHC_VERSION),
    },
}

# =============================================================================
# =============================================================================
# ==                                                                         ==
# ==   TODO: TEMPORARY darwin/arm64 WORKAROUND -- DELETE WHEN GHC >= 9.2      ==
# ==                                                                         ==
# ==   GHC 9.0.2 has NO native code generator for AArch64 (the AArch64 NCG    ==
# ==   first shipped in GHC 9.2.1). On Apple Silicon it therefore falls back  ==
# ==   to the LLVM backend (`-fllvm`), which shells out to `opt`/`llc` and    ==
# ==   only accepts LLVM 9-12. Our hermetic cc toolchain is LLVM 22 -- which  ==
# ==   GHC 9.0.2 cannot use -- so on darwin we bundle a separate LLVM 12      ==
# ==   `opt`/`llc` (the very version the old WORKSPACE/Nix build used) purely ==
# ==   to drive GHC's backend. x86_64 Linux is unaffected (it has an NCG).    ==
# ==                                                                         ==
# ==   >>>>>>>>>>  THE SWITCH  <<<<<<<<<<                                     ==
# ==   When GHC is bumped to >= 9.2 (native AArch64 NCG), set this to False.  ==
# ==   That single flip disables the ENTIRE workaround: no LLVM-12 fetch, no  ==
# ==   copy into the toolchain, no PATH injection -- the toolchain reverts    ==
# ==   to its original, backend-free shape. Nothing else to touch.           ==
# ==                                                                         ==
# ==   Full write-up:                                                        ==
# ==   .bzlregression/issues/002_darwin_arm64_ghc_llvm_backend.md            ==
# ==                                                                         ==
# =============================================================================
# =============================================================================
DARWIN_GHC_LLVM_BACKEND = True

GHC_LLVM_BACKEND_ARTIFACTS = [
    {
        "url": "https://conda.anaconda.org/conda-forge/osx-arm64/llvm-tools-12.0.1-h93073aa_2.tar.bz2",
        "sha256": "39de1566a3ef8a5ec50165aa97ef46b9abb9b9417877d1bd28c4e943a67d5c98",
        "output": "tools",
    },
    {
        "url": "https://conda.anaconda.org/conda-forge/osx-arm64/libllvm12-12.0.1-h93073aa_2.tar.bz2",
        "sha256": "6743583906acce81fe157f735c46bdd8ea2dd5b340f7e2d449c55a070cf85e4d",
        "output": "lib",
    },
]
