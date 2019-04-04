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

    It "java is installed with unlimited cryptography enabled" {
        dadew "install"
        dadew "enable"
        dadew "sync" "$PSScriptRoot\envs\java"

        da_is_installed "java" | Should -Be True
        da_is_installed "jps" | Should -Be True
        da_is_installed "groovy" | Should -Be True

        (groovy -e 'println(javax.crypto.Cipher.getMaxAllowedKeyLength(\"AES\"))') | Out-String | Should -Match ([regex]::Escape("2147483647"))
    }

    It "bazel is installed and responds with version" {
        dadew "install"
        dadew "enable"
        dadew "sync" "$PSScriptRoot\envs\bazel"

        da_is_installed "bazel" | Should -Be True

        (bazel version) | Out-String | Should -Match ([regex]::Escape("Build label: "))
    }
}
