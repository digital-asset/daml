# Note on major macOS versions

The instructions below have been tested on a macOS Catalina host to create a
macOS Catalina guest. While I believe it is possible to _run_ different
combinations of the guest/host macOS versions, the documentation for the
macinbox project is pretty clear that it is not possible to _create_ Catalina
images from earlier OSes, nor earlier OS images from a Catalina host.

As I only have access to Catalina host systems, I have not been able to test
older versions.

# Machine Setup

You should consider the following changes to the MacOS device:

- Disable Energy Saving settings
- Ensure timezone and time sync are set correctly
- Depending on requirements to manage boxes, enable Screen Share and Remote Access

# Installing tools

## macOS Installer App

First thing to do is start the download of the macOS installer app from the App
Store. This will take a while as the installer is rather large, but you can do
most of the other steps below while the download is going on.

I have tested these steps on macOS Catalina 10.15.3 with a Catalina installer
created on 2020-01-23 (15.1.00).

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

These instructions have been tested with 2.2.9.

## Hypervisor Selection

We provide two options for the hypervisor: VirtualBox (open-source) and VMWare Fusion (commercial license). This
resulted from our testing on large Mac Mini nodes (6 Core, 64Gb, 500Gb drives) where we found VirtualBox to be
less stable when attempting to use > 6 virtual cores or 32Gb or more of memory. The Guest OS would hang on boot, or
experience slow processing or network to the extent that it became unusable.

## VMWare Fusion

Purchase a license for

* [VMware Fusion Pro 11.5.3](http://www.vmware.com/products/fusion.html)
* [Vagrant VMware Desktop Provider 2.0.3](https://www.vagrantup.com/vmware/)

Download the installer packages for this software. Install VMWare Fusion per vendor instructions and accept
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

## VirtualBox

Download and install
[VirtualBox](https://download.virtualbox.org/virtualbox/6.1.6/VirtualBox-6.1.6-137129-OSX.dmg),
including the [extension
pack](https://download.virtualbox.org/virtualbox/6.1.6/Oracle_VM_VirtualBox_Extension_Pack-6.1.6.vbox-extpack).

Alternatively, it can be installed from Homebrew using:

```bash
brew cask install virtualbox
```

**This will require sudo access.**

The extension pack has to be manually added after the installation of
VirtualBox; this can be done through the UI, or running:

```bash
V=$(VBoxManage --version | sed 's/r.*//')
sudo VBoxManage extpack install /path/to/download/Oracle_VM_VirtualBox_Extension_Pack-$V.vbox-extpack
```

These instructions have been tested against 6.1.4 and 6.1.6. The extension
version must match the version of VirtualBox itself. As explained above,
VirtualBox does not seem to fully support our production configuration, though
it may still be useful for local testing.

## rbenv

The underlying scripts to create the "blank" macOS guest machine are written in
Ruby, so we need to install some version of Ruby. Ruby does come with macOS,
and has installers in Homebrew, but for the sake of stability we are going to
go with `rbenv`, so we can pin down the exact Ruby version we use. This also
has the advantage of not needing root access to install the `macinbox` gem.
(Though `sudo` still needs to be used to run it.)

On the host machine, for the user that is going to manage the guest VM, add the
following to the relevant shell init file (`~/.zshrc` by default):

```bash
if [ -d $HOME/.rbenv/shims ]; then
    export PATH="$HOME/.rbenv/shims:$PATH"
fi
if [ -d $HOME/.rbenv/bin ]; then
    export PATH="$HOME/.rbenv/bin:$PATH"
fi
```

then run the following:

```bash
git clone https://github.com/rbenv/rbenv.git ~/.rbenv
cd ~/.rbenv
git checkout c6324ff45af33a194f5658a1c6322a94da145f98
```

Note: this is the commit I tested these instructions with; I have no reason to
believe the `master` branch is bad.

In a new terminal (to pick up the new PATH), run:

```bash
mkdir -p "$(rbenv root)"/plugins
git clone https://github.com/rbenv/ruby-build.git "$(rbenv root)"/plugins/ruby-build
cd "$(rbenv root)/plugins/ruby-build"
git checkout 0ef5e055659230e2fb2ae9b0928f70dc27c1c136
```

where, again, this is the commit I tested with, so I am providing it for
maximum reproducibility, but I have no reason to mistrust the `master` branch.

Finally, run:
```bash
rbenv install 2.7.0
rbenv global 2.7.0
```

which installs Ruby 2.7.0 for the current user.

## macinbox

From this directory, run:

```bash
gem install macinbox -v 4.0.0
```

The version I tested with is 4.0.0.

# Creating base image

After all the tools are installed (and the macOS installer has finished
downloading), you can create the base image ("Vagrant box") using the following
command:

## VMWare Fusion Variant

```bash
sudo macinbox --box-format vmware_desktop --disk 250 --memory 57344 --cpu 10 --user-script user-script.sh
```

## VirtualBox variant

NOTE: Limited use of hardware due to possible bugs in VirtualBox.

```bash
sudo macinbox --box-format virtualbox --disk 50 --memory 4096 --cpu 1 --user-script user-script.sh
```

The disk size given here (in GB) will be the disk size used by the individual
VMs created based on this box; memory and cpu parameters are default values
that can be overridden in the Vagrantfile.

200GB disk size is the value we arrived at for Windows and Linux nodes by
progressively incrementing the size each time we had a "disk is full" error.

32GB of RAM and 4 CPU cores are the values I arrived at by running our existing
build against various virtual instance types on Linux and Windows (see
[#4520](https://github.com/digital-asset/daml/pull/4520)).

250Gb disk, 56Gb memory and 10 virtual cores was arrived at from testing on Mac Mini 2018 nodes with 6 core
processors.

The provided "user script", which can be inspected in the current directory,
adds a `synthetic.conf` file as part of the base macOS image we are creating,
such that new VMs will start up with an available mount point on `/nix`.

# Sharing the base box

If you want to share this base box (to avoid having to run the above on every
macOS host), you can copy over the `~/.vagrant.d/boxes/macinbox` directory to
new machines.
