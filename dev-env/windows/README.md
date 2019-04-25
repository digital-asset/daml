# Windows Dev-env

DADEW (DA Dev Env for Windows) is a tool, which can be used to setup a developer's environment on Windows machines.

# How to use it?

First you need to enable long file paths by running the following command in an admin powershell:

```
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name LongPathsEnabled -Type DWord -Value 1
```

Then you need to install your local DADEW environment by running:

    .\dev-env\windows\bin\dadew.ps1 install
    
In this step a [Scoop command-line installer](https://scoop.sh) is downloaded and configured. DADEW by default resides in User's home directory in `dadew` folder, usually `C:\Users\<user-id>\dadew\`.
This is one-off action, unless you `uninstall` your DADEW environment.


To sync your local environment with required target environment's tools run:

    .\dev-env\windows\bin\dadew.ps1 sync <.dadew file>

or just

    .\dev-env\windows\bin\dadew.ps1 sync

in case of `.dadew` file in current location.

This will fetch all required tools, if not already available, and make them ready to use, but they will not be present on the `$PATH` yet.

To make all `sync`'ed tools present on the `$PATH`

    .\dev-env\windows\bin\dadew.ps1 enable

To disable run:

    .\dev-env\windows\bin\dadew.ps1 disable

`$PATH` environment variable is changed only in the current session, so DADEW needs to be enabled each time new powershell session is created in order to get tools available.

DADEW environment can be also uninstalled by running:

    .\dev-env\windows\bin\dadew.ps1 uninstall

which will remove all tools physically from the workstation.

To get an absolute path of a binary provided by this dadew run:

    .\dev-env\windows\bin\dadew.ps1 which <app>

# Versioning

DADEW is versioned. You can check which version you are running by calling:

    .\dev-env\windows\bin\dadew.ps1 version

In order to sync with required tools from specific repo (specific `.dadew` file) you need to have the same or newer version of DADEW installed.
The minimum version which is required in order to perform a sync can be check by calling:

    .\dev-env\windows\bin\dadew.ps1 required-version <.dadew file>

or just

    .\dev-env\windows\bin\dadew.ps1 required-version

in case of `.dadew` file in current location.

# How to build?

    PS C:\> ./build.ps1

This will create `dev-env-windows/dist/dadew-<VERSION>.zip` file.

# How to test?

DADEW is tested with use of Pester 4.

To check if Pester is available on your Windows machine and what version is installed run:

    Get-InstalledModule -Name Pester

this will print Windows module details, including version:

    Version    Name                                Repository           Description
    -------    ----                                ----------           -----------
    4.4.2      Pester                              PSGallery            Pester provides a framework for running BDD styl...

Version 4.x is required to run tests. If it's not present you can use simple script available at `dev-env-windows/test/update-pester.ps1` to install required version of Pester.

Tests can be run using:

    PS C:\> ./test.ps1

