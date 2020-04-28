# Host setup

The host machine for this step needs three things:

1. The "macinbox" [Vagrant base box](../1-create-box/README.md) for macOS.
2. VirtualBox (with the extension) or VMWare.
3. Vagrant (and the VMWare plugin if using VMWare).

As all three are already covered by the `macinbox` step, the easiest approach
is probably to just run this on the same box.

You can refer to the instructions in [this README](../1-create-box/README.md),
or, for a more automated setup, use the following set of commands if using
Virtualbox (which will require a sudoer password at some points):

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)" < /dev/null
brew cask install virtualbox
brew cask install vagrant
cd $(mktemp -d)
V=$(VBoxManage --version | sed 's/r.*//')
wget https://download.virtualbox.org/virtualbox/$V/Oracle_VM_VirtualBox_Extension_Pack-$V.vbox-extpack
sudo VBoxManage extpack install Oracle_VM_VirtualBox_Extension_Pack-$V.vbox-extpack
cd -
```

This will install homebrew, then use it to install the latest versions of
VirtualBox and Vagrant. The `brew cask` invocations need to be ran from a
sudoer account and will request a password (unless passwordless sudo is
configured, which I would not recommend), so this cannot be completely
scripted. `brew` itself will refuse to run if started as root.

# Creating the daily Vagrant box

This step builds upon the `macinbox` step to take a blank macOS image and add:

- The vsts user that will run the Azure agent.
- The XCode CLI tools.
- A working nix installation.
- All the nix dependencies for the daml project.
- A populated Bazel cache for the daml project.

This results in a machine that is completely initialized, without having needed
any credentials. The only bit missing is the actual Azure agent.

This can be run every day, though it is probably good enough to run it once a
week.

To select a provider, you need to set the `PROVIDER` environment variable. If
you do not set it, `vmware_desktop` will be used. The only other valid option
at the moment is `virtualbox`.

The steps to create the box are:

```
vagrant up
vagrant package --output initialized-$(date +%Y%m%d).box
```

The `vagrant up` command can take a while as it is doing all of the internal
setup for the machine. It also ends with shutting down the machine so there
should be no lingering process.

The output file is all that needs to be moved to the other machines; they do
not need the original `macinbox` files.

Once the output file is created, you can also remove the "active" (though at
this point shut down) Vagrant box for this folder by running
```
vagrant destroy
```
