# Working with docs

Visit the  [main README file](./../README.md#5-working-with-docs)

## Daml JSON docs for shared conversion

`daml` owns generation of the JSON docs artifact. Conversion into MDX is owned
by `digital-asset/docs`.

1. Generate the JSON:
   `./sdk/docs/scripts/generate-daml-prim-json.sh`
2. Run the shared converter from a docs checkout:
   `./sdk/docs/scripts/run-shared-daml-docs-json-to-mdx.sh --docs-repo /path/to/docs --output-dir /path/to/docs/docs-main/appdev/reference/daml-prim-api --docs-json /path/to/docs/docs.json`
3. Checked-in JSON location in this repo:
   `sdk/docs/sharable/sdk/reference/daml/stdlib/daml-prim.json`

`./sdk/ci/synchronize-docs.sh` now refreshes this checked-in JSON file together
with the rest of `sdk/docs/sharable`.

Note: JSON generation uses the SDK Bazel workspace/toolchains and is expected to
run inside the DADE (`direnv` + `nix`) development environment.
