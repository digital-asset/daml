
The SDK assistant comes with multiple commands to manage the SDK
installations.

None of the following steps should fail because of a '.DS_Store'
file in the ~/.da/packages/sdk directory.

  $ touch ~/.da/packages/sdk/.DS_Store.

We can for example 'list' the installed SDK versions:

  $ da list
  Installed SDK releases:
    [0-9\.]+ \(active, default\) (re)

Lets first check what the current version looks like:

  $ da path damlc
  /.*/.da/packages/damlc/[^/]+/da-hs-damlc-app (re)
  $ readlink ~/.vscode/extensions/da-vscode-daml-extension
  /.*/.da/packages/daml-extension/.+ (re)

Or we can change the default version (that is used when creating new
projects and for DAML Studio):

  $ da use 0.2.10
  Installing packages:.* (re)
  Activating packages: daml-extension
  Activating SDK version 0.2.10...

