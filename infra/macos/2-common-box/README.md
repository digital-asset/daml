# Host setup

The host machine for this step needs three things:

1. The "macinbox" [Vagrant base box](../1-create-box/README.md) for macOS.
2. VMWare.
3. Vagrant and the VMWare plugin.

As all three are already covered by the `macinbox` step, the easiest approach
is probably to just run this on the same box.

# Creating the daily Vagrant box

This step builds upon the `macinbox` step to take a blank macOS image and add:

- The vsts user that will run the Azure agent.
- The XCode CLI tools.
- A working nix installation.

This results in a machine that is completely initialized, without having needed
any credentials. The only bit missing is the actual Azure agent.

This can be run every day, though it is probably good enough to run it once a
week.

The steps to create the box are:

```
vagrant up
vagrant package --output initialized-$(date +%Y%m%d).box
mv initialized-$(date +%Y%m%d).box ~/images/
vagrant destroy -f
```

The `vagrant up` command can take a while as it is doing all of the internal
setup for the machine. It also ends with shutting down the machine so there
should be no lingering process.

The output file is all that needs to be moved to the other machines; they do
not need the original `macinbox` files. Note that all machines are scripted to
look for the "most recent" (based on filename) file in `~/images` for step 3,
so that's where you should put this.

Once the output file is created, you can also remove the "active" (though at
this point shut down) Vagrant box.
