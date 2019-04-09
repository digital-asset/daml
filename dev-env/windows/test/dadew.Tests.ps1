. "$PSScriptRoot\..\libexec\test-utils.ps1"

function clean {
    Remove-Item $env:DADEW -Recurse -Force -ErrorAction Ignore
    $env:DADEW_USE_COLORS = $null
    da_clear_random_dadew_home
}

function dadew() {
    $env:DADEW_USE_COLORS = "false"
    . $PSScriptRoot/../bin/dadew.ps1 @args
}

Describe "DADEW - DA DevEnv Windows" {

    BeforeEach {
        da_random_dadew_home
        . "$PSScriptRoot\..\libexec\dadew.ps1"
        da_reset_path
    }

    AfterEach {
        clean
    }

    It "can be initialized in custom directory" {
        $env:SCOOP | Should -Match ([regex]::Escape($env:DADEW))
    }

    It "provides Scoop on install" {
        da_is_installed "scoop" | Should -Be False
        dadew "install"
        da_is_installed "scoop" | Should -Be False
        dadew "enable"
        da_is_installed "scoop" | Should -Be True
    }

    It "provides tools listed in .dadew file - jq" {
        da_is_installed "jq" | Should -Be False
        da_is_installed "scoop" | Should -Be False

        dadew "install"
        dadew "enable"
        dadew "sync" "$PSScriptRoot\envs\jq"
        da_is_installed "jq" | Should -Be True
        (jq --version) | Out-String | Should -Match ([regex]::Escape("jq-1.5"))
    }

    It "syncs provides tools listed in .dadew file ensuring not-listed are uninstalled" {
        da_is_installed "jq" | Should -Be False
        da_is_installed "scoop" | Should -Be False

        dadew "install"
        dadew "enable"
        dadew "sync" "$PSScriptRoot\envs\jq"

        da_is_installed "jq" | Should -Be False

        dadew "sync" "$PSScriptRoot\envs\jq"

        da_is_installed "jq" | Should -Be True
    }

    It "can be enabled & disabled" {
        da_is_installed "jq" | Should -Be False
        da_is_installed "scoop" | Should -Be False
        dadew "install"
        dadew "enable"
        dadew "sync" "$PSScriptRoot\envs\jq"
        da_is_installed "jq" | Should -Be True

        dadew "disable"

        da_is_installed "jq" | Should -Be False

        dadew "enable"

        da_is_installed "jq" | Should -Be True
    }

    It "provides shims for same tools with different versions" {
        da_is_installed "jq" | Should -Be False
        da_is_installed "scoop" | Should -Be False
        dadew "install"
        dadew "enable"

        dadew "sync" "$PSScriptRoot\envs\jq15"

        da_is_installed "jq" | Should -Be True
        da_is_installed "jq14" | Should -Be False
        da_is_installed "jq15" | Should -Be True
        (jq --version) | Out-String | Should -Match ([regex]::Escape("jq-1.5"))
        (jq15 --version) | Out-String | Should -Match ([regex]::Escape("jq-1.5"))

        dadew "sync" "$PSScriptRoot\envs\jq14"
        da_is_installed "jq" | Should -Be True
        da_is_installed "jq14" | Should -Be True
        da_is_installed "jq15" | Should -Be False
        (jq --version) | Out-String | Should -Match ([regex]::Escape("jq-1.4"))
        (jq14 --version) | Out-String | Should -Match ([regex]::Escape("jq-1.4"))

        dadew "sync" "$PSScriptRoot\envs\jq15"
        da_is_installed "jq" | Should -Be True
        da_is_installed "jq14" | Should -Be False
        da_is_installed "jq15" | Should -Be True
        (jq --version) | Out-String | Should -Match ([regex]::Escape("jq-1.5"))
        (jq15 --version) | Out-String | Should -Match ([regex]::Escape("jq-1.5"))
    }

    It "is versioned" {
        $version = Get-Content "$PSScriptRoot/../VERSION" | Select -Index 0
        $version | Should -Match "^\d*\.\d*\.\d*$"
    }

    It "provides version of itself" {
        dadew "version" | Should -Be (Get-Content "$PSScriptRoot/../VERSION" | Select -Index 0)
    }

    It "provides version of required dadew" {
        dadew "required-version" "$PSScriptRoot\envs\version" | Should -Be "99.0.91"
    }

    It "fails on sync when required version is higher than version" {
        dadew "install"
        dadew "enable"
        $out = dadew "sync" "$PSScriptRoot\envs\version" | Out-String
        $out | Should -Match ([regex]::Escape("DADEW requires update. Current version: "))
        $out | Should -Match ([regex]::Escape("Required version: 99.0.91"))
    }

    It "fails on sync when configuration file does not exist" {
        dadew "install"
        dadew "enable"
        $out = dadew "sync" "$PSScriptRoot\envs\non-existent" | Out-String
        $out | Should -Match ([regex]::Escape("No .dadew file found at "))
        $out | Should -Match ([regex]::Escape("envs\non-existent\.dadew"))
    }

    It "provides dadew home path" {
        dadew "where" | Should -Be $env:DADEW
    }

    It "provides app binary path" {
        dadew "install"
        dadew "enable"
        dadew "which" scoop | Should -Be "$env:DADEW\scoop\apps\scoop\current\bin\scoop.ps1"
    }
}
