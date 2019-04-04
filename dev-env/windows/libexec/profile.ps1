while (Test-Path Alias:curl) {Remove-Item Alias:curl}
while (Test-Path Alias:wget) {Remove-Item Alias:wget}

$env:HOME = $env:USERPROFILE
$env:LOGONSERVER = 'LOCALHOST'
