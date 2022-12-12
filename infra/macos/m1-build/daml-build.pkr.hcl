packer {
  required_plugins {
    tart = {
      version = ">= 0.3.1"
      source  = "github.com/cirruslabs/tart"
    }
  }
}


variable "vsts_token" {
  type = string
}

variable "guest_name" {
  type = string
}

source "tart-cli" "init-1" {
  cpu_count    = 4
  memory_gb    = 10
  ssh_password = "admin"
  ssh_timeout  = "240s"
  ssh_username = "admin"
  vm_base_name = "monterey-base"
  vm_name      = "monterey-init-1"
}

source "tart-cli" "init-2" {
  cpu_count    = 8
  memory_gb    = 14
  disk_size_gb = 100
  ssh_password = "admin"
  ssh_timeout  = "240s"
  ssh_username = "admin"
  vm_base_name = "monterey-init-1"
  vm_name      = "monterey-init-2"
}

source "tart-cli" "init-3" {
  disk_size_gb = 200
  ssh_password = "admin"
  ssh_timeout  = "240s"
  ssh_username = "admin"
  vm_base_name = "monterey-init-2"
  vm_name      = "monterey-init-3"
}

build {
  name = "init-1"
  sources = ["source.tart-cli.init-1"]

  provisioner "file" {
    sources     = ["init-1.sh"]
    destination = "~/"
  }

  provisioner "shell" {
    expect_disconnect = true
    inline = [
      "sudo bash -c ~/init-1.sh",
    ]
  }
}

build {
  name = "init-2"
  sources = ["source.tart-cli.init-2"]

  provisioner "file" {
    sources     = ["init-2.sh"]
    destination = "~/"
  }

  provisioner "shell" {
    inline = [
      "/usr/sbin/softwareupdate --install-rosetta --agree-to-license",
    ]
  }

  provisioner "shell" {
    expect_disconnect = true
    inline = [
      "sudo bash -c ~/init-2.sh",
    ]
  }
}

build {
  name = "init-3"
  sources = ["source.tart-cli.init-3"]

  provisioner "file" {
    sources     = ["init-3.sh"]
    destination = "~/"
  }

  provisioner "shell" {
    expect_disconnect = true
    inline = [
      join(" ", ["sudo bash -c \"", "~/init-3.sh", var.vsts_token, var.guest_name, "\""])
    ]
  }
}
