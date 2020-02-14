# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  TEMP_garys_token   = "${secret_resource.vsts-token.value}"
  TEMP_garys_account = "digitalasset"
  TEMP_garys_pool    = "garys-windows-pool"
}

resource "google_compute_region_instance_group_manager" "TEMP_garys-agent-windows" {
  provider = "google-beta"
  name     = "TEMP_garys-agent-windows"

  # keep the name short. windows hostnames are limited to 12(?) chars.
  # -5 for the random postfix:
  base_instance_name = "TEMP_garys-win"

  region      = "${local.region}"
  target_size = 1

  version {
    name              = "TEMP_garys-agent-windows"
    instance_template = "${google_compute_instance_template.TEMP_garys-agent-windows.self_link}"
  }

  update_policy {
    type           = "PROACTIVE"
    minimal_action = "REPLACE"

    # minimum is the number of availability zones (3)
    max_surge_fixed = 3

    # calculated with: serial console last timestamp after boot - VM start
    # 09:54:28 - 09:45:55 = 513 seconds
    min_ready_sec = 520
  }
}

resource "google_compute_instance_template" "TEMP_garys-agent-windows" {
  name_prefix  = "TEMP_garys-agent-windows-"
  machine_type = "n1-standard-8"
  labels       = "${local.labels}"

  disk {
    disk_size_gb = 200
    disk_type    = "pd-ssd"

    # find the image name with `gcloud compute images list`
    source_image = "windows-cloud/windows-2016"
  }

  # Drive D:\ for the agent work folder
  disk {
    disk_size_gb = 200
    disk_type    = "pd-ssd"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata {
    // Prepare the machine
    sysprep-specialize-script-ps1 = <<SYSPREP_SPECIALIZE
Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

# Disable Windows Defender to speed up disk access
Set-MpPreference -DisableRealtimeMonitoring $true

# Enable long paths
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name LongPathsEnabled -Type DWord -Value 1

# Disable UAC
New-ItemProperty -Path HKLM:Software\Microsoft\Windows\CurrentVersion\policies\system -Name EnableLUA -PropertyType DWord -Value 0 -Force

# Redirect logs to SumoLogic

cd $env:UserProfile;
Invoke-WebRequest https://dl.google.com/cloudagents/windows/StackdriverLogging-v1-9.exe -OutFile StackdriverLogging-v1-9.exe;
.\StackdriverLogging-v1-9.exe /S /D="C:\Stackdriver\Logging\"

# Install chocolatey
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
iex (New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1')

# Install git, bash
& choco install git --no-progress --yes 2>&1 | %{ "$_" }
& choco install windows-sdk-10.1 --no-progress --yes 2>&1 | %{ "$_" }

# Add tools to the PATH
$OldPath = (Get-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH).path
$NewPath = "$OldPath;C:\Program Files\Git\bin;C:\Program Files (x86)\Windows Kits\10\App Certification Kit"
Set-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH -Value $NewPath

echo "== Prepare the D:\ drive"

$partition = @"
select disk 1
clean
convert gpt
create partition primary
format fs=ntfs quick
assign letter="D"
"@
$partition | Set-Content C:\diskpart.txt
& diskpart /s C:\diskpart.txt 2>&1 | %{ "$_" }

# Create a temporary and random password for the VSTS user, forget about it once this script has finished running
$Username = "VssAdministrator"
$Account = "$env:COMPUTERNAME\$Username"
Add-Type -AssemblyName System.Web
$Password = [System.Web.Security.Membership]::GeneratePassword(24, 0)

echo "== Creating the VSTS user"

#New-LocalUser $Username -Password $SecurePassword -FullName $Username
net user $Username $Password /add /y
# net localgroup administrators $Username /add
Add-LocalGroupMember -Group "Administrators" -Member $Username

winrm set winrm/config/winrs '@{MaxMemoryPerShellMB="2048"}'
winrm set winrm/config '@{MaxTimeoutms="1800000"}'
winrm set winrm/config/service/auth '@{Basic="true"}'
net stop winrm
sc.exe config winrm start=auto
net start winrm

echo "== Installing the VSTS agent"

choco install azure-pipelines-agent --no-progress --yes --params "'/Token:${local.TEMP_garys_token} /Pool:${local.TEMP_garys_pool} /Url:https://dev.azure.com/${local.TEMP_garys_account}/ /LogonAccount:$Account /LogonPassword:$Password /Work:D:\a'"
echo OK
SYSPREP_SPECIALIZE

    windows-shutdown-script-ps1 = "c://agent/config remove --unattended --auth PAT --token '${secret_resource.vsts-token.value}'"
  }

  network_interface {
    network = "default"

    // Ephemeral IP to get access to the Internet
    access_config {}
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = false
  }
}
