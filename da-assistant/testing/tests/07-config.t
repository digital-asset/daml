
Test that all the expected configuration keys are there:

  $ da config-help
  The following properties are configurable in a da.yaml file:
  version                  The config file version. This helps the SDK
                           Assistant manage backwards compatibility of da.yaml
                           files.
  
  
  cli.script-mode          Run SDK Assistant in non-interactive mode
                           without attempting self-upgrade.
  cli.update-channel       Update channel to use for the SDK Assistant
                           self-updates.
  user.email               Your email address.
  project.name             The project name.
  project.sdk-version      The project SDK version.
  project.parties          Names of parties active in the project.
  project.source           The DAML file the Sandbox should load.
  project.scenario         Optional: Scenario that the Sandbox should run.
                           For example: Example.myScenario
  bintray.username         Your Bintray username. Required in order for
                           the SDK Assistant to download SDK releases.
  bintray.key              Your Bintray API Key. Required in order for the
                           SDK Assistant to download SDK releases.
  bintray.api-url          The Bintray API URL.
  bintray.download-url     The Bintray download URL.
  sdk.default-version      The SDK version to use by default when starting
                           new projects and similar.
  sdk.included-tags        Only SDK releases with at least one of these
                           tags will be available.
  sdk.excluded-tags        SDK releases tagged with one of these tags will
                           be ignored.
  term.width               Rendering width of the terminal.
  
  templates.namespaces     The SDK template namespaces you are subscribed
                           to.
  update-setting           The current self-update setting. Possible
                           values are: Always, RemindLater, RemindNext, Never.
  project.output-path      Output path for DAML compilation.

Test that we can retrieve some common config entries:

  $ da config get version
  2
  
  $ da config get cli.script-mode
  false
  
  $ da config get user.email
  test@digitalasset.com
  
  $ da config get project.name
  [Error] Configuration property not found for 'project.name'
  [1]
  $ da config get project.sdk-version
  [Error] Configuration property not found for 'project.sdk-version'
  [1]
  $ da config get project.parties
  [Error] Configuration property not found for 'project.parties'
  [1]
  $ da config get project.source
  [Error] Configuration property not found for 'project.source'
  [1]
  $ da config get project.scenario
  [Error] Configuration property not found for 'project.scenario'
  [1]

  $ da config get bintray.api-url
  http://localhost:8882
  
  $ da config get bintray.download-url
  http://localhost:8882
  
  $ da config get sdk.default-version
  0.10.2
  
  $ da config get sdk.included-tags
  - visible-external
  
  $ da config get sdk.excluded-tags
  - bad
  - deprecated
  
  $ da config get term.width
  [Error] Configuration property not found for 'term.width'
  [1]

Test that it also sets global configuration values:

  $ da config set version 10
  Cannot set property 'version'
  [1]
  $ da config set cli.script-mode true
  Cannot set property 'cli.script-mode'
  [1]
  $ da config set user.email new.email@digitalasset.com
  $ da config get user.email
  new.email@digitalasset.com
  
  $ da config set user.email test@digitalasset.com
  $ da config set bintray.api-url https://my.bintray.com
  $ da config get bintray.api-url
  https://my.bintray.com
  
  $ da config set bintray.api-url https://localhost:8882
  $ da config set bintray.download-url https://my.digitalasset.com
  $ da config get bintray.download-url
  https://my.digitalasset.com
  
  $ da config set bintray.download-url https://localhost:8882
  $ da config set sdk.default-version 0.2.1
  $ da config get sdk.default-version
  0.2.1
  
  $ da config set sdk.default-version 0.10.2
  $ da config set sdk.included-tags tag1,tag2
  $ da config get sdk.included-tags
  - tag1
  - tag2
  
  $ da config set sdk.included-tags tag1,,tag2
  Party cannot be empty
  [1]
  $ da config set sdk.included-tags visible-external
  $ da config get sdk.included-tags
  - visible-external
  
  $ da config set sdk.excluded-tags tag3,tag4
  $ da config get sdk.excluded-tags
  - tag3
  - tag4
  
  $ da config set sdk.excluded-tags tag3,,tag4
  Party cannot be empty
  [1]
  $ da config set sdk.excluded-tags bad,deprecated
  $ da config set term.width 2000
  Cannot set property 'term.width'
  [1]

Test that it also shows project configuration when we're in a project:

  $ da new config-test
  Creating a project called config-test
  in /.*/07-config.t/config-test. (re)
  $ cd config-test
  $ da config get project.name
  config-test
  
  $ da config get project.source
  daml/Main.daml
  
  $ da config set --project project.name my-project
  $ da config get project.name
  my-project
  
  $ da config set --project project.source daml/Main2.daml
  $ da config get project.source
  daml/Main2.daml
  
  $ da config set --project user.email new.email2@digitalasset.com
  $ da config get user.email
  new.email2@digitalasset.com
  
  $ da config set --project bintray.api-url https://my.bintray.com.2
  $ da config get bintray.api-url
  https://my.bintray.com.2
  
  $ da config set --project bintray.download-url https://my.digitalasset.com.2
  $ da config get bintray.download-url
  https://my.digitalasset.com.2
  
  $ da config set --project sdk.default-version 0.2.0
  $ da config get sdk.default-version
  0.2.0
  
  $ da config set --project sdk.included-tags tag5,tag6
  $ da config get sdk.included-tags
  - tag5
  - tag6
  
  $ da config set --project sdk.excluded-tags tag7,tag8
  $ da config get sdk.excluded-tags
  - tag7
  - tag8
  
  $ cd ..
  $ da config get version
  2
  
  $ da config get cli.script-mode
  false
  
  $ da config get user.email
  test@digitalasset.com
  
  $ da config get bintray.api-url
  https://localhost:8882
  
  $ da config get bintray.download-url
  https://localhost:8882
  
  $ da config get sdk.default-version
  0.10.2
  
  $ da config get sdk.included-tags
  - visible-external
  
  $ da config get sdk.excluded-tags
  - bad
  - deprecated
  
