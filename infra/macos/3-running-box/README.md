# Host setup

Same as for [step 2](../2-common-box/README.md).

# Running a node

With Vagrant and VirtualBox/VMWare installed, running a box is as simple as moving to
this folder and running

```
vagrant box add azure-ci-node /path/to/initialized.box
GUEST_NAME=... VSTS_TOKEN=... vagrant up
```

which should start the base box and run the [init script](init.sh), which in
turn should set up the machine and connect to Azure. The command will return
when the machine has successfully finished its own setup, and is about to
connect to Azure to register as a CI node. Any error before that will result in
the command returning a non-zero exit code.

Refer to [step 2](../2-common-box/README.md) for how to create the initialized
box.

The `VSTS_TOKEN` environment variable must be set to the token used to register
nodes with Azure. It is the same for all CI nodes (across OSes).

The `GUEST_NAME` environment variable is used as the name of the agent when
connecting to Azure, i.e. that is the name that will be displayed in the Azure
web UI to identify this machine. It should be different for each CI node (e.g.
`macOS-1`, `macOS-2`).

Logs from the Azure agent (which mostly consist of the names of jobs that get
started and whether they complete successfully or not) are piped to a file on
the guest VM and can be tailed from the host with:

```
vagrant ssh -c "tail -f run.log"
```

Destroying the machine is done by running:

```
vagrant destroy -f
```

which will forcibly shut down the machine and delete all the associated files
(for the running VM; the base box is used read-only and untouched by this
command).

Vagrant boxes are tied to the folder they are started from, so running multiple
CI nodes as multiple guest VMs on the same host can be done by having multiple
copies of this folder and running `vagrant up` in each of them.
