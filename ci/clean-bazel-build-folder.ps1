$ErrorActionPreference = 'Stop'

$buildDir = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\daml_build'))
$outputBase = 'C:\b'
$dumpedPaths = @('disk_cache', 'repository_cache', 'user_root' | ForEach-Object { Join-Path $buildDir $_ }) + $outputBase
$driveRoots = @($buildDir, $outputBase | ForEach-Object { [System.IO.Path]::GetPathRoot($_) } | Select-Object -Unique)

function Write-BuildFolderDump {
  foreach ($path in $dumpedPaths) {
    if (Test-Path -LiteralPath $path) {
      $entries = @(Get-ChildItem -LiteralPath $path -Force -ErrorAction SilentlyContinue | ForEach-Object Name)
      Write-Host "present: $path ($($entries.Count) entries: $($entries -join ', '))"
    } else {
      Write-Host "absent: $path"
    }
  }
  foreach ($root in $driveRoots) {
    $drive = New-Object System.IO.DriveInfo $root
    Write-Host ('{0} free: {1:N1} GB of {2:N1} GB' -f $drive.Name, ($drive.AvailableFreeSpace / 1GB), ($drive.TotalSize / 1GB))
  }
}

function Clear-LongPathFolder($path) {
  $emptyDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
  New-Item -ItemType Directory -Path $emptyDir | Out-Null
  robocopy $emptyDir $path /MIR /XJ /R:0 /W:0 /NFL /NDL /NJH /NJS /NP
  $robocopyExitCode = $LASTEXITCODE
  Remove-Item -LiteralPath $emptyDir
  if ($robocopyExitCode -ge 8) { throw "robocopy could not empty $path (exit code $robocopyExitCode)" }
}

function Stop-OutputBaseServer {
  $pidFile = Join-Path $outputBase 'server\server.pid.txt'
  if (-not (Test-Path -LiteralPath $pidFile)) { return }
  $serverPid = 0
  if (-not [int]::TryParse("$(Get-Content -LiteralPath $pidFile -Raw)".Trim(), [ref]$serverPid)) { return }
  $server = Get-CimInstance Win32_Process -Filter "ProcessId = $serverPid"
  if (-not $server -or $server.CommandLine -notlike '*A-server.jar*') { return }
  Write-Host "stopping Bazel server $serverPid left in $outputBase, with its workers"
  & taskkill.exe /PID $serverPid /T /F | Out-Null
  Wait-Process -Id $serverPid -Timeout 60 -ErrorAction SilentlyContinue
}

Write-BuildFolderDump
Stop-OutputBaseServer
if (Test-Path -LiteralPath $buildDir) { Clear-LongPathFolder $buildDir }
foreach ($path in @($buildDir, $outputBase)) {
  if (Test-Path -LiteralPath $path) {
    cmd /c rd /s /q "\\?\$path"
    if (Test-Path -LiteralPath $path) { throw "could not remove $path" }
  }
}
Write-BuildFolderDump
