Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

$info = Get-CimInstance -ClassName win32_operatingsystem

$info | format-table CSName

$info | format-table LastBootUpTime

$info | format-table FreePhysicalMemory, TotalVisibleMemorySize

get-wmiobject win32_logicaldisk | format-table `
    Name,`
    @{n="Size [GB]";e={[math]::truncate($_.size / 1GB)}},`
    @{n="Free [GB]";e={[math]::truncate($_.freespace / 1GB)}}
