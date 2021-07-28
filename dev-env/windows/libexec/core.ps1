. "$PSScriptRoot\init.ps1"

function da_is_scoop_on_path {
    $result = $env:Path -match [regex]::escape($scoopShimDir)
    return $result
}

function da_is_installed([String] $cmdName) {
    $cmd = (Get-Command "$cmdName" -ErrorAction SilentlyContinue)
    return !($cmd -eq $null) -and !($cmd.CommandType -eq "Alias")
}

function da_is_scoop_installed {
    return da_is_installed "scoop"
}

function da_enable_scoop {
    if (-Not (da_is_scoop_on_path)) {
        da_reset_path
        $env:PATH = "$PSScriptRoot\..\bin;$scoopShimDir;$env:PATH" # for this session
        . "$PSScriptRoot\profile.ps1"
    }
}

function da_reset_path {
    $env:PATH = [Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [Environment]::GetEnvironmentVariable("PATH", "User")
}

function da_enable_win_dev_env {
    da_enable_scoop
    if (da_is_scoop_installed) {
        da_auto_prevent_update
    } else {
        da_error "dadew is not installed, run dadew install"
        da_reset_path
    }
}

function da_auto_prevent_update {
    if (da_is_scoop_on_path) {
        scoop config lastupdate $([System.DateTime]::Now).ToString('o')
    }
}

function da_clear_buckets {
    Remove-Item $scoopInstallDir\buckets\dadew -r -force -ErrorAction Ignore
    New-Item $scoopInstallDir\buckets\dadew -ItemType "directory" -Force
}

function da_sync_buckets([String] $Directory) {
    da_clear_buckets
    $files = Get-ChildItem -Path $Directory -Name | Where-Object {$_ -like '*.json'}
    ForEach ($file in $files) {
        Get-Content "$Directory\$file" | Set-Content "$scoopInstallDir\buckets\dadew\$file"
    }
}

function da_sync_scoop([String] $Directory) {
    da_enable_win_dev_env
    da_install_all $Directory
}

function da_install_all([String] $Directory) {
    $confFilePath = "$Directory\.dadew"
    If (-Not ([System.IO.File]::Exists($confFilePath))) {
        da_error "No .dadew file found at $confFilePath"
        return
    }

    da_auto_prevent_update

    $currentVersion = da_version
    $requiredVersion = da_required_version $Directory
    If ((da_compare_version $currentVersion $requiredVersion) -lt 0) {
        da_error "DADEW requires update. Current version: $currentVersion. Required version: $requiredVersion."
        return
    }

    $config = (Get-Content $confFilePath | ConvertFrom-Json)

    If ($config.manifests) {
        $manifestsDir = $Directory + "\" + $config.manifests
        da_info "Found 'manifests' configuration option pointing to: '$manifestsDir'."
        da_info "Performing manifests sync ..."
        If (-Not (Test-Path $manifestsDir)) {
            da_error "Manifests directory not found at '$manifestsDir'."
            return
        }
        da_sync_buckets -Directory $manifestsDir
        da_success "Manifests synced."
    }

    $installedApps = New-Object System.Collections.Generic.HashSet[string]
    scoop export | Foreach-object {
        $app = $_.split(" ", 2)[0]
        [void]$installedApps.Add($app)
    }

    $requiredApps = New-Object System.Collections.Generic.HashSet[string]
    $config.tools | ForEach-Object {
        [void]$requiredApps.Add($_)
    }

    $appsToUninstall = New-Object System.Collections.Generic.HashSet[string]($installedApps)
    $appsToUninstall.ExceptWith($requiredApps)

    $appsToUninstall | ForEach-Object {
        da_info ">> scoop uninstall $_"
        scoop uninstall $_
    }

    $requiredApps | ForEach-Object {
        $app = $_
        da_info ">> Installing '$app' ..."
        $out = (scoop install $_ *>&1) | Out-String
        $out = $out -join "`r`n" | Out-String
        $prevInstallFailed = $out -like "*It looks like a previous installation of $app failed*"
        $alreadyInstalled = $out -like "*is already installed.*"
        $justInstalled = $out -like "*was installed successfully!*"

        If ($prevInstallFailed) {
            da_warn "$app installation failed - trying to reinstall"
            $reinstalled,$out = da_reinstall_app $app
            If ($reinstalled -eq $True) {
                da_success "<< ok"
            } Else {
                $msg = "$app installation failed after clean install."
                da_error "<< $msg"
                da_error $out
                throw $msg
            }
        } ElseIf ($alreadyInstalled) {
            $installedSha = ""
            If (Test-Path $scoopInstallDir\apps\$app\current\manifest.json) {
                $installedSha = (Get-FileHash $scoopInstallDir\apps\$app\current\manifest.json).Hash
            }

            $availableSha = (Get-FileHash $scoopInstallDir\buckets\dadew\$app.json).Hash

            If ($installedSha -eq $availableSha) {
                da_success "<< ok"
            } Else {
                da_warn "$app manifest has changed - reinstalling the app ..."
                $reinstalled,$out = da_reinstall_app $app
                If ($reinstalled -eq $True) {
                    da_success "<< ok"
                } Else {
                    $msg = "$app installation failed:"
                    da_error "<< $msg"
                    da_error $out
                    throw $msg
                }
            }
        } ElseIf ($justInstalled) {
            da_success "<< ok"
        } Else {
            $msg = "<< Unknown state: `r`n$out"
            da_error $msg
            throw $msg
        }

        $resetted,$out = da_reset_app $app
        If (-not($resetted)) {
            $msg = "<< Resetting $app failed: `r`n$out"
            da_error $msg
            throw $msg
        }
    }
}

function da_reset_app([String] $app) {
    $out = (scoop reset $app *>&1)
    $out = $out -join "`r`n" | Out-String

    $resettingFound = $out -like "*Resetting*"
    $errorFound = $out -like "*ERROR*"

    If (-not($resettingFound) -or $errorFound) {
        return $False, $out
    } Else {
        return $True, $out
    }
}

function da_reinstall_app([String] $app) {
    $out = (scoop uninstall $app *>&1)
    $out = (scoop install $app *>&1)
    $out = $out -join "`r`n" | Out-String

    $alreadyInstalled = $out -like "*is already installed.*"
    $justInstalled = $out -like "*was installed successfully!*"

    If (-not($alreadyInstalled -or $justInstalled)) {
        return $False, $out
    } Else {
        return $True, $out
    }
}

function da_uninstall_app([String] $app) {
    da_info ">> Uninstalling $app"
    $out = (scoop uninstall $app *>&1) | Out-String
    $uninstalled = $out -like "*was uninstalled*"
    If ($uninstalled) {
        da_success "<< ok"
    } Else {
        da_error "An error occurred uninstalling the '$app':"
        da_error $out
    }
}

function da_resolve_fullpath([String] $Path) {
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Path)
}

function da_version {
    (Get-Content "$PSScriptRoot/../VERSION" | Select -Index 0).Trim()
}

function da_required_version([String] $Directory) {
    (Get-Content "$Directory\.dadew" | ConvertFrom-Json).version
}

function da_compare_version([String] $lhs, [String] $rhs) {
    $lhs -match '^(\d*)\.(\d*)\.(\d*)' > $null
    $lv = $Matches

    $rhs -match '^(\d*)\.(\d*)\.(\d*)' > $null
    $rv = $Matches

    If ($lv[1] -eq $rv[1]) {
        If ($lv[2] -eq $rv[2]) {
            If ($lv[3] -eq $rv[3]) {
                return 0
            } Else {
                return $lv[3] - $rv[3]
            }
        } Else {
            return $lv[2] - $rv[2]
        }
    } Else {
        return $lv[1] - $rv[1]
    }
}

function da_where {
    return $dadewInstallDir
}

function da_which([String] $App) {
    $appHome = scoop which $App
    if (!$appHome) {
        return
    }
    return $appHome -replace ([regex]::escape("~\")), "$env:USERPROFILE\"
}

function da_use_colors {
    $default = $true
    try {
        If ($env:DADEW_USE_COLORS -eq $null) { return $default }
        return [System.Convert]::ToBoolean($env:DADEW_USE_COLORS)
    } catch [FormatException] {
        return $true
    }
}
function da_msg([String] $msg, [String] $color) { If (da_use_colors) { Write-Host $msg -f $color } Else { Write-Output $msg } }
function da_debug([String] $msg) { da_msg $msg "darkcyan" }
function da_info([String] $msg) { da_msg $msg "cyan" }
function da_warn([String] $msg) { da_msg $msg "yellow" }
function da_error([String] $msg) { da_msg $msg "red" }
function da_success([String] $msg) { da_msg $msg "darkgreen" }
