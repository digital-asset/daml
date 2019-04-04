
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

Now we should see two versions installed:

  $ da list
  Installed SDK releases:
    0.2.10 (active, default)
    [0-9\.]+. (re)

# ## Not in use ##
# # We cannot install the real 0.2.10 and we cannot really build it
# And we should be using 'damlc' from version 0.2.0:
# 
#   $ da path damlc
#   /.*/.da/packages/damlc/13.0.1/da-hs-damlc-app (re)
#   $ `da path damlc`
#   Missing: COMMAND
#   
#   Usage: da-hs-damlc-app COMMAND
#     Invoke the DAML compiler. Use -h for help.
#   [1]
# 
# 
# The version of daml-extension should also now point to
# an older version:
# 
#   $ readlink ~/.vscode/extensions/da-vscode-daml-extension
#   /.*/.da/packages/daml-extension/13.0.1 (re)
If there is no new version, no upgrade takes place:

  $ da upgrade
  Checking for latest version... 0.10.3
  Already installed: 0.10.3, checking packages:
  Activating packages: daml-extension
  Activating SDK version 0.10.3...
  0.10.3
    Date: 2018-10-16
    Changelog:
    - Fix Maven repository config when using the Java binding templates
    - Update the Java binding docs with the step to add credentials to the local Maven settings

  $ curl --request GET -s -w "%{http_code}" -o /dev/null --url http://localhost:8882/makeNewSdkAssistantAvailable
  200 (no-eol)

  $ cat ~/.da/da.yaml | grep update-setting
  update-setting: Never

  $ da --version
  Installed SDK Assistant version: 0-aaa
  .{0} (re)
  Type `da update-info` to see information about update channels.

However, the user can explicitly update the Assistant:

  $ da upgrade
  Performing self-update. For more info on updates type `da update-info`.
  Found latest version: 1-aaa
  Downloading installer.
  SDK Assistant 1-aaa installed.
  Run 'da setup' to start using the SDK Assistant.
  Update installed. Re-launching the CLI.
  Checking for latest version... 0.10.3
  Already default version: 0.10.3
  0.10.3
    Date: 2018-10-16
    Changelog:
    - Fix Maven repository config when using the Java binding templates
    - Update the Java binding docs with the step to add credentials to the local Maven settings

Update can be configurated such that it is always automatically performed:

  $ da config set update-setting Always
  $ cat ~/.da/da.yaml | grep update-setting
  update-setting: Always
  $ curl --request GET -s -w "%{http_code}" -o /dev/null --url http://localhost:8882/makeNewSdkAssistantAvailable
  200 (no-eol)

  $ rm -rf ~/.da/last-check
  $ da list
  Version of SDK Assistant to be installed: 2-aaa
  Performing self-update. For more info on updates type `da update-info`.
  Found latest version: 2-aaa
  Downloading installer.
  SDK Assistant 2-aaa installed.
  Run 'da setup' to start using the SDK Assistant.
  Update installed. Re-launching the CLI.
  Installed SDK releases:
    0.2.10 
    0.10.3 (active, default)

The user can skip specific versions with RemindNext. By default, it will remind about
the next version counted from the actual. The user can skip this version which means
that upgrade to it won't be offered once again.

  $ da config set update-setting RemindNext
  $ cat ~/.da/da.yaml | grep update-setting
  update-setting: RemindNext
  $ curl --request GET -s -w "%{http_code}" -o /dev/null --url http://localhost:8882/makeNewSdkAssistantAvailable
  200 (no-eol)

  $ rm -rf ~/.da/last-check
  $ echo 3 | da list
  Version of SDK Assistant to be installed: 3-aaa
  Do you want to install the new SDK Assistant version 3-aaa? (Answer with a number, 1..5)
  (1) Yes, always install new versions
  (2) Yes, but ask again for next version
  (3) No, but ask again for next version
  (4) No, but ask again for this version
  (5) No, never install new versions
  Installed SDK releases:
    0.2.10 
    0.10.3 (active, default)
  $ da list
  Installed SDK releases:
    0.2.10 
    0.10.3 (active, default)

The next version, however, will be offered to the user who can also answer with RemindLater:

  $ curl --request GET -s -w "%{http_code}" -o /dev/null --url http://localhost:8882/makeNewSdkAssistantAvailable
  200 (no-eol)

  $ rm -rf ~/.da/last-check
  $ echo 4 | da list
  Version of SDK Assistant to be installed: 4-aaa
  Do you want to install the new SDK Assistant version 4-aaa? (Answer with a number, 1..5)
  (1) Yes, always install new versions
  (2) Yes, but ask again for next version
  (3) No, but ask again for next version
  (4) No, but ask again for this version
  (5) No, never install new versions
  Installed SDK releases:
    0.2.10 
    0.10.3 (active, default)

No reminder will be shown before time:

  $ rm -rf ~/.da/last-check
  $ da list
  Installed SDK releases:
    0.2.10 
    0.10.3 (active, default)

But after a while, the Assistant starts showing reminders:

  $ rm -rf ~/.da/last-check
  $ rm -rf ~/.da/last-upgrade-reminder
  $ echo 1 | da list
  Version of SDK Assistant to be installed: 4-aaa
  Do you want to install the new SDK Assistant version 4-aaa? (Answer with a number, 1..5)
  (1) Yes, always install new versions
  (2) Yes, but ask again for next version
  (3) No, but ask again for next version
  (4) No, but ask again for this version
  (5) No, never install new versions
  Performing self-update. For more info on updates type `da update-info`.
  Found latest version: 4-aaa
  Downloading installer.
  SDK Assistant 4-aaa installed.
  Run 'da setup' to start using the SDK Assistant.
  Update installed. Re-launching the CLI.
  Installed SDK releases:
    0.2.10 
    0.10.3 (active, default)

And now the path to 'damlc' points to a newer version:

  $ da path damlc
  /.*/.da/packages/damlc/[^\/]+/da-hs-damlc-app (re)
