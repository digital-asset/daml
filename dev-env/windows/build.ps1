Remove-Item $PSScriptRoot\dist -Recurse -Force -ErrorAction Ignore
New-Item -ItemType directory -Path $PSScriptRoot\dist
$version = (Get-Content "$PSScriptRoot/VERSION" | Select -Index 0).Trim()
Compress-Archive -Path $PSScriptRoot\bin, $PSScriptRoot\manifests, $PSScriptRoot\libexec, $PSScriptRoot\VERSION -CompressionLevel Fastest -Update -DestinationPath $PSScriptRoot\dist\dadew-$version.zip
