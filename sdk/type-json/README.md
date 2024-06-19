
# Type Signature Json Schema

## Contents

This directory contains:

- A [JSON Schema](./typesig.schema.json) definition for the type signature JSON produced by `com.daml.lf.typesig.EncoderDecoder`

- An [example](./example.json) of type signature JSON for `ledger-tests-model.dar`

## Running

Any schema validator that supports the `draft-07` schema syntax can be used. For manual testing [ajv](https://www.npmjs.com/package/ajv-cli) was used. For example:

```
$ ajv validate -s typesig.schema.json -d example.json
example.json valid
```

## TODO

- Move to the `2020-12` version of json-schema.

