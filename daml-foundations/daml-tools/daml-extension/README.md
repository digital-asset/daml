# DAML Studio (__NIX_GIT_REV__)

DAML Studio extends Visual Studio Code with the following DAML-specific
features.

- DAML syntax highlighting
- Real-time feedback on parse, name resolution, type-checking and
  Scenario interpretation errors and viewer for the resulting ledger
- Jumping to and peeking at the definition of referenced toplevel functions
- Type-information on hover
- Renaming of symbols
- DAML snippet support

For more information on DAML please see docs.daml.com.

## Troubleshooting

The DAML language server log output is available under the "Output" panel (View->Output).
Select "DAML Language Server" from the dropdown in the panel to see the log.

## Debugging

Run `make` then open this directory in Visual Studio Code. Then click Debug -> Start Debugging to run the extension in Debugging mode.
