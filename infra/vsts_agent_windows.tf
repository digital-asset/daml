# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  vsts_token   = secret_resource.vsts-token.value
  vsts_account = "digitalasset"
  vsts_pool    = "windows-pool"
}

locals {
  w = [
    {
      name       = "ci-w1",
      size       = 6,
      assignment = "default",
      disk_size  = 400,
    },
    {
      name       = "ci-w2"
      size       = 0,
      assignment = "default",
      disk_size  = 400,
    },
  ]
  windows-startup-script-ps1 = <<SYSPREP_SPECIALIZE
Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

# Disable Windows Defender to speed up disk access
Set-MpPreference -DisableRealtimeMonitoring $true

# Disable Print Spooler service (security)
Stop-Service -Name Spooler -Force
Set-Service -Name Spooler -StartupType Disabled

# Disable File & Printer sharing
Set-NetFirewallRule -DisplayGroup "File And Printer Sharing" -Enabled False -Profile Any

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
& choco install git --no-progress --yes 2>&1 | %%%{ "$_" }
& choco install windows-sdk-10.1 --no-progress --yes 2>&1 | %%%{ "$_" }

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
& diskpart /s C:\diskpart.txt 2>&1 | %%%{ "$_" }

# Create a temporary and random password for the VSTS user, forget about it once this script has finished running
$Username = "u"
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

& choco install dotnetcore-3.1-sdk --no-progress --yes 2>&1 | %%%{ "$_" }

echo "== Installing the VSTS agent"

New-Item -ItemType Directory -Path 'C:\agent'
Set-Content -Path 'C:\agent\.capabilities' -Value 'assignment=%s'

# Set workdir <> job mappings
# This is taken verbatim from a machine that started without any custom content
# under SourceRootMapping, and had run all three jobs.
New-Item -ItemType Directory -Path 'D:\a'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\33'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\33\e55e0f7708a35956d0fcb3ec3f6087d9e2e1cd4b'
Set-Content -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\33\e55e0f7708a35956d0fcb3ec3f6087d9e2e1cd4b\SourceFolder.json' `
  -Value '{
  "build_artifactstagingdirectory": "1\\a",
  "agent_builddirectory": "1",
  "collectionUrl": "https://dev.azure.com/digitalasset/",
  "definitionName": "digital-asset.daml-daily-compat",
  "repositoryTrackingInfo": [
    {
      "identifier": "self",
      "repositoryType": "GitHub",
      "repositoryUrl": "https://github.com/digital-asset/daml",
      "sourcesDirectory": "1\\s\\daml"
    }
  ],
  "fileFormatVersion": 3,
  "lastRunOn": "01/26/2022 06:02:24 +00:00",
  "repositoryType": "GitHub",
  "lastMaintenanceAttemptedOn": "",
  "lastMaintenanceCompletedOn": "",
  "build_sourcesdirectory": "1\\s",
  "common_testresultsdirectory": "1\\TestResults",
  "collectionId": "cb88e308-485c-40f9-81b5-dcabba9e55d2",
  "definitionId": "33",
  "hashKey": "e55e0f7708a35956d0fcb3ec3f6087d9e2e1cd4b",
  "repositoryUrl": "https://github.com/digital-asset/daml",
  "system": "build"
}'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\39'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\39\55f020fa694a8855ee0847c05346f0bcf9a1308d'
Set-Content -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\39\55f020fa694a8855ee0847c05346f0bcf9a1308d\SourceFolder.json' `
  -Value '{
  "build_artifactstagingdirectory": "2\\a",
  "agent_builddirectory": "2",
  "collectionUrl": "https://dev.azure.com/digitalasset/",
  "definitionName": "PRs",
  "repositoryTrackingInfo": [
    {
      "identifier": "self",
      "repositoryType": "GitHub",
      "repositoryUrl": "https://github.com/digital-asset/daml",
      "sourcesDirectory": "2\\s\\daml"
    }
  ],
  "fileFormatVersion": 3,
  "lastRunOn": "01/26/2022 15:46:13 +00:00",
  "repositoryType": "GitHub",
  "lastMaintenanceAttemptedOn": "",
  "lastMaintenanceCompletedOn": "",
  "build_sourcesdirectory": "2\\s",
  "common_testresultsdirectory": "2\\TestResults",
  "collectionId": "cb88e308-485c-40f9-81b5-dcabba9e55d2",
  "definitionId": "39",
  "hashKey": "55f020fa694a8855ee0847c05346f0bcf9a1308d",
  "repositoryUrl": "https://github.com/digital-asset/daml",
  "system": "build"
}'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\4'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\4\7d0e386214448f3482003fba798ec97e2a2d504f'
Set-Content -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\4\7d0e386214448f3482003fba798ec97e2a2d504f\SourceFolder.json' `
  -Value '{
  "build_artifactstagingdirectory": "3\\a",
  "agent_builddirectory": "3",
  "collectionUrl": "https://dev.azure.com/digitalasset/",
  "definitionName": "digital-asset.daml",
  "repositoryTrackingInfo": [
    {
      "identifier": "self",
      "repositoryType": "GitHub",
      "repositoryUrl": "https://github.com/digital-asset/daml",
      "sourcesDirectory": "3\\s\\daml"
    }
  ],
  "fileFormatVersion": 3,
  "lastRunOn": "01/28/2022 11:43:49 +00:00",
  "repositoryType": "GitHub",
  "lastMaintenanceAttemptedOn": "",
  "lastMaintenanceCompletedOn": "",
  "build_sourcesdirectory": "3\\s",
  "common_testresultsdirectory": "3\\TestResults",
  "collectionId": "cb88e308-485c-40f9-81b5-dcabba9e55d2",
  "definitionId": "4",
  "hashKey": "7d0e386214448f3482003fba798ec97e2a2d504f",
  "repositoryUrl": "https://github.com/digital-asset/daml",
  "system": "build"
}'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\40'
New-Item -ItemType Directory -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\40\da86374d7fbb90b47db2b7fe49e04373d7a0f7dc'
Set-Content -Path 'D:\a\SourceRootMapping\cb88e308-485c-40f9-81b5-dcabba9e55d2\40\da86374d7fbb90b47db2b7fe49e04373d7a0f7dc\SourceFolder.json' `
  -Value '{
  "build_artifactstagingdirectory": "4\\a",
  "agent_builddirectory": "4",
  "collectionUrl": "https://dev.azure.com/digitalasset/",
  "definitionName": "digital-asset.daml.daily-snapshot",
  "repositoryTrackingInfo": [
    {
      "identifier": "self",
      "repositoryType": "GitHub",
      "repositoryUrl": "https://github.com/digital-asset/daml",
      "sourcesDirectory": "4\\s\\daml"
    }
  ],
  "fileFormatVersion": 3,
  "lastRunOn": "02/14/2022 11:38:15 +00:00",
  "repositoryType": "GitHub",
  "lastMaintenanceAttemptedOn": "",
  "lastMaintenanceCompletedOn": "",
  "build_sourcesdirectory": "4\\s",
  "common_testresultsdirectory": "4\\TestResults",
  "collectionId": "cb88e308-485c-40f9-81b5-dcabba9e55d2",
  "definitionId": "40",
  "hashKey": "da86374d7fbb90b47db2b7fe49e04373d7a0f7dc",
  "repositoryUrl": "https://github.com/digital-asset/daml",
  "system": "build"
}'
Set-Content -Path 'D:\a\SourceRootMapping\Mappings.json' -Value '{
  "lastBuildFolderCreatedOn": "02/14/2022 11:38:15 +00:00",
  "lastBuildFolderNumber": 4
}'
# end folder pinning

$MachineName = Get-CimInstance -ClassName Win32_OperatingSystem | Select-Object CSName | ForEach{ $_.CSName }
choco install azure-pipelines-agent --no-progress --yes --params "'/Token:${local.vsts_token} /Pool:${local.vsts_pool} /Url:https://dev.azure.com/${local.vsts_account}/ /LogonAccount:$Account /LogonPassword:$Password /Work:D:\a /AgentName:$MachineName /Replace'"
echo OK
SYSPREP_SPECIALIZE
}

resource "google_compute_region_instance_group_manager" "vsts-agent-windows" {
  count    = length(local.w)
  provider = google-beta
  name     = local.w[count.index].name

  # keep the name short. windows hostnames are limited to 12(?) chars.
  # -5 for the random postfix:
  base_instance_name = local.w[count.index].name

  region      = "us-east1"
  target_size = local.w[count.index].size

  version {
    name              = local.w[count.index].name
    instance_template = google_compute_instance_template.vsts-agent-windows[count.index].self_link
  }

  # uncomment when we get a provider >3.55
  #distribution_policy_target_shape = "ANY"

  update_policy {
    type           = "PROACTIVE"
    minimal_action = "REPLACE"

    # minimum is the number of availability zones (3)
    max_surge_fixed = 3

    # calculated with: serial console last timestamp after boot - VM start
    # 09:54:28 - 09:45:55 = 513 seconds
    min_ready_sec = 520

    instance_redistribution_type = "NONE"
  }
}

resource "google_compute_instance_template" "vsts-agent-windows" {
  count        = length(local.w)
  name_prefix  = "${local.w[count.index].name}-"
  machine_type = "c2-standard-8"
  labels       = local.machine-labels

  disk {
    disk_size_gb = local.w[count.index].disk_size
    disk_type    = "pd-ssd"

    # find the image name with `gcloud compute images list`
    source_image = "windows-cloud/windows-2016"
  }

  # Drive D:\ for the agent work folder
  disk {
    disk_size_gb = local.w[count.index].disk_size
    disk_type    = "pd-ssd"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata = {
    // Prepare the machine
    windows-startup-script-ps1  = nonsensitive(format(local.windows-startup-script-ps1, local.w[count.index].assignment))
    windows-shutdown-script-ps1 = nonsensitive("c://agent/config remove --unattended --auth PAT --token '${secret_resource.vsts-token.value}'")
  }

  network_interface {
    network = "default"

    // Ephemeral IP to get access to the Internet
    access_config {}
  }

  service_account {
    scopes = ["cloud-platform"]
    email  = "log-writer@da-dev-gcp-daml-language.iam.gserviceaccount.com"
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = false
  }
}
