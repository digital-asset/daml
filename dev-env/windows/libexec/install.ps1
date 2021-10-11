. "$PSScriptRoot\core.ps1"

function da_install {
    if(($PSVersionTable.PSVersion.Major) -lt 3) {
        da_error "PowerShell 3 or greater is required to run DA Windows DevEnv."
        da_error "Upgrade PowerShell: https://docs.microsoft.com/en-us/powershell/scripting/setup/installing-windows-powershell"
        return
    }

    if([System.Enum]::GetNames([System.Net.SecurityProtocolType]) -notcontains 'Tls12') {
        da_error "DA Windows DevEnv requires at least .NET Framework 4.5"
        da_error "Please download and install it first:"
        da_error "https://www.microsoft.com/net/download"
        return
    }

    da_enable_scoop
    $isScoopEnabled = da_is_scoop_installed
    da_reset_path

    if ($isScoopEnabled) {
        da_info "Scoop is already installed"
    } else {
        da_info "Installing Scoop - a command-line installer for Windows ..."

        Remove-Item $scoopInstallDir -Recurse -Force -ErrorAction Ignore

        Add-Type -AssemblyName System.IO.Compression.FileSystem
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

        Remove-Item $scoopTmpDir -Recurse -Force -ErrorAction Ignore
        New-Item -ItemType Directory -Force -Path $scoopTmpDir | Out-null
        (new-object net.webclient).downloadFile($scoopUrl, $scoopTmp)

        [System.IO.Compression.ZipFile]::ExtractToDirectory($scoopTmp, $scoopTmpDir)

        . $scoopCore

        $scoopFinalDir = ensure (versiondir 'scoop' 'current')

        Copy-Item "$scoopMaster\*" $scoopFinalDir -r -force

        Remove-Item $scoopTmpDir -r -force

        shim "$scoopFinalDir\bin\scoop.ps1" $false

        ensure_robocopy_in_path

        da_sync_buckets $PSScriptRoot\..\manifests

        success "Scoop was installed successfully!"
    }
}
