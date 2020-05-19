# Introduction

While our Linux and Windows machines are using standard cloud infrastructure,
and as such can be created entirely from Terraform scripts, our macOS nodes
must be managed on a more physical level. This folder contains all the
instructions needed to create a virtual macOS machine (running on a physical
macOS host) that can be added to our Azure pool.

There are a few pieces to this puzzle:

1. [Instructions to create a base Vagrant box](1-create-box/README.md). This only
   needs to be done once per Apple-supplied macOS installer version; the
   resulting base box can be shared to new machines by simply copying a folder.
1. [Vagrantfile and init script](2-vagrant-files/README.md); this is the piece
   that will, from the box defined above, start up a brand new macOS VM,
   install everything we need, run a complete build of the project, and then
   connect to Azure and wait for CI build requests.
3. Additional considerations, discussed below.

# Security considerations

The guest machine is created with a user, `vagrant`, that has passwordless
`sudo` access and can be accessed with the default, well-known Vagrant SSH
"private" key and a well known default password. While this is useful for debugging, 
it is crucial that the SSH port of the guest machine MUST NOT be accessible from 
outside the host machine, and that access to the host machine itself be appropriately 
restricted.

My personal recommendation would be for the host machines to not be accessible
from any network, and to instead be managed by physical access, if possible.

The `init.sh` script creates a `vsts` user with more restricted access to run
the CI builds. NOTE: the VSTS agent islocked down so upgrade from the Azure console 
will fail. Expectation is that the nodes are cycled daily and will pick up latest
Azure VSTS agent on rebuild.

# Machine initialization

The Vagrantfile for CI nodes will read the init script directly from the master
branch on GitHub. This means that any change to [the init
script](2-vagrant-files/init.sh) for the macOS machines constitutes a "Standard
Change", just like changes to the Linux and Windows scripts. Note that this
should already be enforced by virtue of the `init.sh` file being under the
already-monitored-by-CI folder `//infra`.

The intention is that the macOS nodes, like the Linux and Windows ones, should
be cycled daily, so changing their config file can be done with no other human
intervention than committing to the master branch. This means that, without
additional human intervention, changes would take about a day to propagate,
whereas the DAML team can apply changes to the Linux and Windows nodes directly
through Terraform. I believe this tradeoff is necessary, because we should
restrict access to the underlying macOS hosts as much as practical. Ideally,
they should not be reachable from the internet at all.

# Wiping nodes on a cron

Just like the Linux and Windows nodes, the macOS nodes should be wiped daily.
This is the main reason for putting in the effort in virtualizing them, as
opposed to just setting up physical nodes directly.

As explained in the [Vagrant section](2-vagrant-files/README.md) of this
document, this should be as simple as adding a cron to run

```
cd path/to/Vagrantfile/folder && vagrant destroy -f && GUEST_NAME=... VSTS_TOKEN=... vagrant up
```

every day at 4AM UTC (to synchronize with the Linux and Windows ones).

# Proxying the cache

It is likely (though not certain) that, at some point, we will want to reduce
the amount of traffic generated between our macOS CI nodes and the GCP-hosted
caches, both for performance and price reasons. Under VirtualBox, guest VMs by
default use their host machines as the default gateway, so this should be
feasible through standard HTTP proxying.

However, I have not yet spent much time investigating this.

# Other virtualization techniques

While this folder suggests one known-to-work way to get CI nodes, there are
alternative options. I have not spent much time exploring other virtualization
options or other ways to create an initial "blank" macOS virtual hard drive,
though I believe the provided `init.sh` script should work with most other
approaches with minimal changes, if required.
