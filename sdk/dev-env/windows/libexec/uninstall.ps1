. "$PSScriptRoot\core.ps1"

function da_uninstall {
    da_enable_win_dev_env

    If ((Get-Command "scoop" -ErrorAction SilentlyContinue) -eq $null) {
        da_error "DA Windows DevEnv is not installed."
        return
    }

    da_info "Uninstalling all apps ..."
    scoop export | Foreach-object {
        $app = $_.split(" ", 2)[0]
        da_uninstall_app $app
    }

    Remove-Item $scoopInstallDir -Recurse -Force -ErrorAction SilentlyContinue

    If (Test-Path($scoopInstallDir)) {
        da_error "Some files could not be deleted. Check $scoopInstallDir folder."
    } Else {
        da_info "All files deleted"
    }
}
