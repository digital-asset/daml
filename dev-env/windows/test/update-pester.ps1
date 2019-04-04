Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.208 -MaximumVersion 2.8.5.208 -Force
Install-Module -Name Pester -Force -SkipPublisherCheck -MinimumVersion 4.4.2 -MaximumVersion 4.4.2
Get-InstalledModule -Name Pester