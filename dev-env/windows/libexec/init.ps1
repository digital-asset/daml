$scoopUrl = 'https://github.com/lukesampson/scoop/archive/b819876ec373cfadc1fc490b56340dec73dc6eb5.zip'

$scoopTmpDir = "$env:Temp\scoop"
$scoopTmp = "$scoopTmpDir\scoop.zip"
$scoopMaster = "$scoopTmpDir\scoop-b819876ec373cfadc1fc490b56340dec73dc6eb5"
$scoopCore = "$scoopMaster\lib\core.ps1"
$dadewInstallDir = $env:DADEW, "$env:USERPROFILE\dadew" | Select-Object -first 1
$scoopInstallDir = "$dadewInstallDir\scoop"
$scoopShimDir = "$scoopInstallDir\shims"

# ensure Scoop is aware of it's non-default install dir
$env:SCOOP=$scoopInstallDir
