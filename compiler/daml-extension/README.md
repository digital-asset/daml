# Daml Studio

Daml Studio extends Visual Studio Code with the following Daml-specific
features:

- Daml syntax highlighting
- Real-time feedback on parse, name resolution, type-checking and
  Scenario interpretation errors and viewer for the resulting ledger
- Jumping to and peeking at the definition of referenced toplevel functions
- Type-information on hover
- Renaming of symbols
- Daml snippet support
- Command to generate visualization for Daml project via command palette ctrl + p.

Please note that this will only install the VSCode extension. Full use of the
above features will also require that you have a working Daml SDK installed,
which you can get with:

```
curl -s https://get.daml.com | sh
```

To see graphs from `daml.visualize` command please install [Graphivz plugin](https://marketplace.visualstudio.com/items?itemName=EFanZh.graphviz-preview).

For more information on Daml please see [docs.daml.com](https://docs.daml.com).

## Troubleshooting

The Daml language server log output is available under the "Output" panel
(View->Output). Select "Daml Language Server" from the dropdown in the panel
to see the log.

## Debugging

Run `make` then open this directory in Visual Studio Code. Then click Debug ->
Start Debugging to run the extension in Debugging mode.
