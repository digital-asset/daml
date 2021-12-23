## Patches

- `ghc-daml-prim.patch` rename `ghc-prim` to `daml-prim`
    Patch generated with
    ```
    BASE=da-master-8.8.1
    git clone https://github.com/digital-asset/ghc.git
    cd ghc
    git checkout $BASE
    git merge --no-edit 833ca63be2ab14871874ccb6974921e8952802e
    git diff $BASE
    ```
