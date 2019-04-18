# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  vsts_token   = "${secret_resource.vsts-token.value}"
  vsts_account = "digitalasset"
  vsts_pool    = "windows-pool"
}

resource "google_compute_region_instance_group_manager" "vsts-agent-windows" {
  provider = "google-beta"
  name     = "vsts-agent-windows"

  # keep the name short. windows hostnames are limited to 12(?) chars.
  # -5 for the random postfix:
  base_instance_name = "vsts-win"

  region      = "${local.region}"
  target_size = 10

  version {
    name              = "vsts-agent-windows"
    instance_template = "${google_compute_instance_template.vsts-agent-windows.self_link}"
  }

  update_policy {
    type           = "PROACTIVE"
    minimal_action = "REPLACE"

    # minimum is the number of availability zones (3)
    max_surge_fixed = 3

    # calculated with: serial console last timestamp after boot - VM start
    # 13:04:27 - 12:57:12 ~= 8 * 60 = 480
    min_ready_sec = 480
  }
}

resource "google_compute_instance_template" "vsts-agent-windows" {
  name_prefix  = "vsts-agent-windows-"
  machine_type = "n1-standard-8"
  labels       = "${local.labels}"

  disk {
    disk_size_gb = 200
    disk_type    = "pd-ssd"

    # find the image name with `gcloud compute images list`
    source_image = "windows-cloud/windows-2016-core"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata {
    // Prepare the machine
    sysprep-specialize-script-ps1 = <<SYSPREP_SPECIALIZE
Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

# Install chocolatey
iex (New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1')

# Install git and bash for azure pipelines
& choco install git.portable --yes 2>&1 | %{ "$_" }

# Add git and bash to the PATH
$OldPath = (Get-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH).path
$NewPath = "$OldPath;C:\tools\git\bin"
Set-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH -Value $NewPath

# Enable long paths
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name LongPathsEnabled -Type DWord -Value 1

# Disable Windows Defender to speed up disk access
Set-MpPreference -DisableRealtimeMonitoring $true

# Create a temporary and random password for the VSTS user, forget about it once this script has finished running
$Username = "vssadministrator"
$Account = "$env:COMPUTERNAME\$Username"

Add-Type -AssemblyName System.Web
$Password = [System.Web.Security.Membership]::GeneratePassword(24, 0)

echo "== Creating the VSTS user"

net user $Username $Password /add /y
net localgroup administrators $Username /add
winrm set winrm/config/winrs '@{MaxMemoryPerShellMB="2048"}'
winrm set winrm/config '@{MaxTimeoutms="1800000"}'
winrm set winrm/config/service/auth '@{Basic="true"}'
net stop winrm
sc.exe config winrm start=auto
net start winrm

echo "== Installing the VSTS agent"

choco install azure-pipelines-agent --yes --params "'/Token:${local.vsts_token} /Pool:${local.vsts_pool} /Url:https://${local.vsts_account}.visualstudio.com /LogonAccount:$Account /LogonPassword:$Password'"
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
    preemptible         = true
  }
}
