$scoopUrl = 'https://github.com/lukesampson/scoop/archive/a9fa775d59b14e7dce335313faa0eff855469764.zip'

$scoopTmpDir = "$env:Temp\scoop"
$scoopTmp = "$scoopTmpDir\scoop.zip"
$scoopMaster = "$scoopTmpDir\scoop-a9fa775d59b14e7dce335313faa0eff855469764"
$scoopCore = "$scoopMaster\lib\core.ps1"
$dadewInstallDir = $env:DADEW, "$env:USERPROFILE\dadew" | Select-Object -first 1
$scoopInstallDir = "$dadewInstallDir\scoop"
$scoopShimDir = "$scoopInstallDir\shims"

# ensure Scoop is aware of it's non-default install dir
$env:SCOOP=$scoopInstallDir
