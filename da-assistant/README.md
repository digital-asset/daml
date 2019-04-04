
DA SDK command-line application
===============================

The Digital Asset SDK command-line application (`da`) is a tool for developers
to help them work with the DA SDK. It installs tools, libraries, sets up and
runs projects, configures access credentials, looks for updates, performs
upgrades, and so on.

Building
--------

The project uses `bazel` as build system but it is a good idea to not use `bazel`
to get feedback from `ghc` about files changed because `bazel` will take minutes
to recompile the project even on small changes. Use `ghcid` instead.

To setup `ghcid` to get feedback about files changed, run from the root
directory of the repository the command:

    ghcid --command="da-ghci da-assistant/DA/Sdk/Cli.hs"

To build the SDK Assistant instead run:

    bazel run //da-assistant:da

To compile and run the installer:

    bazel run //da-assistant:installer.run

To run ghci use the provided wrapper `ghci.sh`. This build the
code generated files and executes `da-ghci` with correct flags.

Deploying and update channels
-----------------------------

The SDK Assistant support separate update channels. Currently, we use two:
"pre-release" and "production". An update channel maps to a Bintray package.
"production" means "da-cli", and any other means "da-cli-",
and the channel as a postfix.
E.g., the "pre" channel translates to the "da-cli-pre" package on Bintray.
It could be set by an optional setting in the configuration file:

    cli:
      script-mode: false
      update-channel: production # <----

In the absence of this setting, the Assistant would default to "pre"
if the "user.email" setting ends in "@digitalasset.com",
and to "production" otherwise.

This way DA employees would be subscribed to the pre-release channel by default,
and they could also change it easily by running

    $ da set cli.update-channel production

The Assistant's self-upgrade logic keeps the Assistant in sync with the top
of the current update channel.
If the version in use differs from the top of the update channel,
the Assistant switches to that version.
It solves the problem of downgrading when switching back to the production
channel (e.g., because of an upcoming demo depending on a since deprecated feature
in pre-release), or that of stepping back a version if we've found a defective
version is on the top of the pre-release channel
and we've removed it (similar to "bad" tags for the SDK).

We release to the pre-release channel, and a separate Jenkins job moves a selected version
to the production channel in case we would publish it for the public.
We should not publish updates to the pre-release channel of inferior quality.
We still release there from the master branch, so the usual quality requirements apply.
The goal is to have another safety net and the possibility of early feedback from internal custumers.

To deploy a new version go to http://ci.da-int.net/job/sdk/job/da-cli/job/release/
and click 'Build now'.

The deployment process is defined by the Jenkins pipeline in
`sdk/scripts/da-cli.Jenkinsfile`, which in turn calls
`sdk/scripts/release-da-cli.sh` to build and deploy the tool.

Version numbers are can be set by editing `da-assistant/VERSION-NUMBER`.

Final versions (including git hash postfixes) are generated automatically
by the deployment pipeline by overriding `da-assistant/VERSION`.
This file should not be edited by hand.

To publish a version from the pre-release channel to the production, go to
http://ci.da-int.net/job/sdk/job/da-cli/job/publish-to-production/.

Rationale
---------

Building applications on top of the DA ledger usually involves many different
technologies and tools: sandbox, DAML, DAML studio, shell, bots, UI framework,
and the Navigator for example. Each developer needs to not only install and
configure these tools, but learn about upgrades and migrate between versions.

One way to accomplish this is to ask developers to follow clear documentation
that uses standard command line tools. The advantage of this is that it's
completely transparent to developers what happens on the system. The
disadvantage is that it requires significant effort not directly related to
understanding and writing DAML applications on the part of the developer.

Another way to accomplish these tasks is to provide developers with a tailor
made command line tool for the DA SDK that can inspect their environment,
automate SDK-specific tasks, and guide the developer through choices the tool
can't figure out on its own. This is more work that writing documentation, but
can significantly simplify and speed up the developer experience.

One way to think about the `da` command-line application is as a personal SDK
assistant for the developer. It should help and advice the developer on how to
achieve SDK tasks, with the goal of making the experience of building DAML
applications enjoyable. We consider this a valuable goal that justifies the time
and effort required to build a polished command-line application.

Versioning
----------

The command-line tool is versioned independently of other parts of the SDK and
will check for and install updates automatically -- developers are meant to have
as recent a version as possible installed and not more than one version at a
time. In contrast, developers can have more than one SDK distribution installed
on their system at one time. This is because DAML projects are tied to specific
versions of the sandbox/ledger and it should be possible to both develop new
projects against the latest ledger and maintain legacy projects.

Publishing templates
--------------------

DA SDK command-line application can be used to upload templates to our code
repositories. Templates are pre-made DAML example modules which show advanced
uses of DAML and provide useful library functions. A template is either project
or add-on template depending whether there is a da.yaml in its project directory
or not. Further info about subscribing to template namespaces, listing templates,
starting new projects from them and adding them to existing projects can be
found in the SDK Assistant manual.

To upload a new template:

  $ da template publish QUALIFIED_TEMPLATE

where `QUALIFIED_TEMPLATE` is an identifier of the form
`<namespace>/<name>/<release-line>` A release line is an SDK version of the
form a.b.x, e.g.: 1.2.x. Letter 'x' means that the template supports all patch
versions from 1.2.x. Some templates, however, only support one specific
version. In case of such a template the <release-line> parameter is of the form
a.b.c, e.g.: 1.2.3

Note that publishing with the same QUALIFIED_TEMPLATE parameter twice results in
a new revision of an already existing template. Users will only see the latest
revision. The type of a template is decided by the existence of a da.yaml file
in the project directory of the latest revision.

To list all available templates (of both types):

    $ da template list

To get info about a specific, published template:

    $ da template info QUALIFIED_TEMPLATE

Windows support
---------------

Setup one needs:
- Start your Windows system
- Install Haskell Stack
- Install Python

To build da-cli with Stack:
- Open a terminal
- Change directory to ..<da>/da-assistant
- Run stack_build.py

System dependencies
-------------------

I used the `ubuntu:16.04` docker image and had to install the following
packages to make `sh da-cli-X-Y`, `da setup` and `da upgrade` run successfully:

- bsdmainutils
- ca-certificates
- netbase
