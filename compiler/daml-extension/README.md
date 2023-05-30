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

Please note that this will only install the VSCode extension. Full use of the
above features will also require that you have a working Daml SDK installed,
which you can get with:

```
curl -s https://get.daml.com | sh
```

For more information on Daml please see [docs.daml.com](https://docs.daml.com).

## Troubleshooting

The Daml language server log output is available under the "Output" panel
(View->Output). Select "Daml Language Server" from the dropdown in the panel
to see the log.

To see the LSP messages sent from and to the client (the vscode
extension) change your VSCode workspace settings in
`.vscode/settings.json` to the following:

```
{
   "daml-language-server.trace.server": "verbose"
}
```

The logs are then included in the `Daml Language Server` logs.

## Debugging

Run `make` then open this directory in Visual Studio Code. Then click Debug ->
Start Debugging to run the extension in Debugging mode.
