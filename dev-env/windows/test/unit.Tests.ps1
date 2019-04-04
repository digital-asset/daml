. $PSScriptRoot/../libexec/core.ps1

Describe "da_compare_version" {

    It "compares semver versions correctly - 0.0.0 = 0.0.0" {
        da_compare_version "0.0.0" "0.0.0" | Should -Be 0
    }

    It "compares semver versions correctly - 0.0.0 < 0.0.2" {
        da_compare_version "0.0.0" "0.0.2" | Should -BeLessThan 0
    }

    It "compares semver versions correctly - 0.0.2 > 0.0.0" {
        da_compare_version "0.0.2" "0.0.0" | Should -BeGreaterThan 0
    }

    It "compares semver versions correctly - 0.1.0 = 0.1.0" {
        da_compare_version "0.1.0" "0.1.0" | Should -Be 0
    }

    It "compares semver versions correctly - 0.1.0 < 0.1.1" {
        da_compare_version "0.1.0" "0.1.1" | Should -BeLessThan 0
    }

    It "compares semver versions correctly - 1.1.2 > 1.1.0" {
        da_compare_version "1.1.2" "1.1.0" | Should -BeGreaterThan 0
    }

    It "compares semver versions correctly - 10.1.2 > 9.1.2" {
        da_compare_version "10.1.2" "9.1.2" | Should -BeGreaterThan 0
    }

    It "compares semver versions correctly - 10.1.2 > 09.1.2" {
        da_compare_version "10.1.2" "09.1.2" | Should -BeGreaterThan 0
    }

    It "compares semver versions correctly - 010.1.2 > 09.1.2" {
        da_compare_version "010.1.2" "09.1.2" | Should -BeGreaterThan 0
    }

    It "compares semver versions correctly - 5.0.0 > 4.9.999" {
        da_compare_version "5.0.0" "4.9.999" | Should -BeGreaterThan 0
    }

}

Describe "da_use_colors" {

    It "should use color mode by default" {
        $env:DADEW_USE_COLORS = $null
        da_use_colors | Should -Be $true
    }

    It "should handle invalid values" {
        $env:DADEW_USE_COLORS = "aaa"
        da_use_colors | Should -Be $true
    }

    It "should handle opt-out" {
        $env:DADEW_USE_COLORS = "false"
        da_use_colors | Should -Be $false
    }

}