while (Test-Path Alias:curl) {Remove-Item Alias:curl}

$env:HOME = $env:USERPROFILE
$env:LOGONSERVER = 'LOCALHOST'
