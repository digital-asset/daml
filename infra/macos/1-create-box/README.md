# Note on major macOS versions

The instructions below have been tested on a macOS Monterey (12.7) host to
create a macOS Catalina guest.

> :warning: Note that the old `macinbox` approach does not work anymore: it is
> not able to create Catalina images from a Montery host, and does not know
> about any OS version more recent than Catalina.
>
> (If you're not sure what this refers to, see the git history of this file if
> you're curious, or just ignore it and move on to how things do work as of
> October 2023.)

# Machine Setup

You should consider the following changes to the MacOS device:

- Disable Energy Saving settings
- Ensure timezone and time sync are set correctly
- Depending on requirements to manage boxes, enable Screen Share and Remote Access

# Host tools

## Homebrew

Homebrew is _not_ required for these instructions, but it can make some of the
following steps easier to automate. Given that, on the one hand, some steps
have to be done manually through the UI anyway, and, on the other hand, this
process really only needs to be done once, on one machine, there is little
value in installing Homebrew for this process. However, in case you already
have Homebrew installed, I do mention the corresponding commands when they
exist.

Note that if you have Homebrew installed, you also already have the CLI tools
and can skip the next step.

## XCode CLI Tools

Within a graphical environment, run:

```bash
xcode-select --install
```

This will open a graphical dialog to install the XCode Command-Line Tools.

## Vagrant

To easily manage the guest VM from scripts, we will be using [Vagrant](https://releases.hashicorp.com/vagrant/2.2.7/vagrant_2.2.9_x86_64.dmg).

Alternatively, installing with Homebrew:

```
brew cask install vagrant
```

These instructions have been tested with 2.3.4.

## VMware Fusion

Purchase a license for

* [VMware Fusion Pro 11.5.3](http://www.vmware.com/products/fusion.html)
* [Vagrant VMware Desktop Provider 2.0.3](https://www.vagrantup.com/vmware/)

Download the installer packages for this software. Install VMware Fusion per vendor instructions and accept
the security settings in Catalina.

In a Terminal, ensure you have vagrant 2.2.9 and upgrade if necessary or you will receive error from vagrant plugin
on VM creation.

```bash
vagrant --version
brew cask install vagrant
```

You will need to install the vagrant plugin and license

```bash
vagrant plugin install vagrant-vmware-desktop
vagrant plugin license vagrant-vmware-desktop ~/license.lic
```

To verify the license installation, run:

```bash
vagrant plugin list
```

200Gb disk, 56Gb memory and 10 virtual cores was arrived at from testing on Mac Mini 2018 nodes with 6 core
processors.


# Creating the Guest VM

## macOS Installer App

On more recent macOS versions, the UI won't let you get older installers. You
can still get the Catalina image with the following CLI command:

```
softwareupdate --fetch-full-installer --full-installer-version 10.15.7
```

This will save the Catalina installer at `/Applications/Install macOS
Catalina.app`.

## Creating a VMware Image

Through the VMware Fusion UI, one can start the "New Virtual Machine" wizard,
then drag-and-drop the Catalina installer from the Applications folder onto the
first screen of the wizard ("Select the Installation Method").

## Machine settings

On machine startup, the installer will be loaded on a secondary hard drive, as
if it were a recovery partition. To get the desired settings (200GB hard drive,
10 processors, 57344MB of RAM), one needs to create the machine, then change
its settings, and then, at first startup, use the Disk Utility option of the
Recovery Tools to resize ("Erase") the main hard drive (`Macintosh HD`).

One then needs to go through the Catalina installer, which can take some time.
Select United States, skip Apple ID. Use the `vagrant` account name and the
host password. Don't import anything, skip Apple ID, don't enable Siri, etc.
Basically say no to anyting you can say no to.

## OS Upgrade

Once Catalina is up and running, we want to upgrade to a more recent version.
Upgrading to Sonoma, as suggested, doesn't work (the installation hangs). But
we can still get some improved security by upgrading to Monterey by running:

```
sudo softwareupdate --fetch-full-installer --full-installer-version 12.7
```

Note that, on my machine at least, the keybpard mappings are all wrong when
connected to the nested VM, and I had to use the accessibility keyboard.

Also note that the above will print error messages, but seems to still proceed
with the install. Once the ugrade is finished, we need to turn the VM into a
Vagrant box.

There are two missing steps for that:

1. Enabling SSH with the [Vagrant insecure key].
2. Installing the [VMware Tools] inside the VM.

See the [Vagrant documentation] for full details on how to turn a VMware
machine into a Vagrant box.

[Vagrant insecure key]: https://github.com/hashicorp/vagrant/blob/main/keys/vagrant.pub
[VMware Tools]: https://kb.vmware.com/s/article/340
[Vagrant documentation]: https://developer.hashicorp.com/vagrant/docs/providers/vmware/boxes


## SSH Server

To enable the SSH server, go to System Preferences -> Sharing and enable Remote
Login. Make sure to add the vagrant user to the list of users authorized to
login through SSH.

Next, we need to add the Vagrant insecure key to `$HOME/.ssh/authorized_keys`.

## Installing the VMware Tools

On the host top-bar menu, with the VM running, click on Virtual Machine ->
Install VMware Tools, then follow the wizard. Let the installer do its thing,
then go to System Preferences -> Security and Privacy to enable the kernel
modules VMware Tools just installed. Then restart the VM.

## Sudo Access

For some of the next steps, we'll need sudoer access without a password:

```bash
sudo bash -c "echo 'vagrant ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers"
```

Note that Azure Pipelines jobs will run as the `vsts` user, not tha `vagrant`
user, so they won't have sudoer access.

## Nix Mountpoint

In order for macOS to create the required mountpoint for Nix, we need to add a
`synthetic.conf` file:

```bash
sudo bash -c 'echo "nix" > "/etc/synthetic.conf"'
```

## Memory Settings

It's unclear whether this is still required as we've been doing this for the
past 5 years without questioning it, but we add these settings on our macOS
nodes:

```bash
sudo bash <<BASH
cat <<SYS > /etc/sysctl.conf
kern.sysv.shmmax=16777216
kern.sysv.shmmin=1
kern.sysv.shmmni=128
kern.sysv.shmseg=32
kern.sysv.shmall=4096
SYS
BASH
```

Do a final reboot then properly shut down the VM from within.

# Turning the VMware VM into a Vagrant Box

This is a fairly simple process. First, make sure the VM is properly shut down.
Assuming the VM is at :
```
$HOME/Virtual Machines.localized/base-20231018.vmwarevm
```
one can create the Vagrant Box with (on the host):

```bash
cd $(mktemp -d)
mkdir tmp
echo '{"provider": "vmware_desktop"}' > tmp/metadata.json
cp $HOME/Virtual\ Machines.localized/base-20231018.vmwarevm/{*.vmdk,*.nvram,*.vmsd,*.vmx,*.vmxf} tmp/
/Applications/VMware\ Fusion.app/Contents/Library/vmware-vdiskmanager -d tmp/*.vmdk
/Applications/VMware\ Fusion.app/Contents/Library/vmware-vdiskmanager -k tmp/*.vmdk
cd tmp
GZIP=-9 tar czf base.box ./*
```

And that's it, we have a base box.

I (Gary Verhaegen) have ran through these instructions on 2023-10-18 and
created a base box with the following hash:

```bash
296ae2b4f78547c4e6e299a4ae9b1cba4873bcadac49a9bc5b3fa4978ae5f834  base.box
```

which I have pushed to GCS at:

```
https://console.cloud.google.com/storage/browser/_details/daml-data/manual/gary/macos/base-2023-10-18.box;tab=live_object?project=da-dev-gcp-daml-language
```

# Sharing the base box

If you want to share this base box (to avoid having to run the above on every
macOS host), you can copy over the `~/.vagrant.d/boxes/base` directory to new
machines, or copy over the `base.box` file and do `vagrant box add` on each
machine. Note that this is not particularly recommended: the box we really want
to have on each machine is the result of step 2.

# Next

In order for this step to flow seamlessly with the existing step 2, we import
the machine under the old `macinbox` name:

```
vagrant box add --name macinbox base.box
```
