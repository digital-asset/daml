# Introduction

While our Linux and Windows machines are using standard cloud infrastructure,
and as such can be created entirely from Terraform scripts, our macOS nodes
must be managed on a more physical level. This folder contains all the
instructions needed to create a virtual macOS machine (running on a physical
macOS host) that can be added to our Azure pool.

There are a few pieces to this puzzle:

1. [Instructions to create a base Vagrant box](1-create-box/README.md). This only
   needs to be done once per Apple-supplied macOS installer version; this is as
   close as we can get to an unmodified, vanilla, out-of-the-box macOS
   installation. (We do add the `synthetic.conf` file though.)
2. [Common tools Vagrant box](2-common-box/README.md). This is a Vagrant box
   created on top of the previous step that does all the initialization except
   for the installation of the Azure agent. This allows us to only do the common
   steps once. This may be rebuilt on a slower frequency, say once a week.
1. [Azure runner](2-running-box/README.md) starts from the previous one and
   downloads and runs the Azure agent. This is the one that should run on each
   machine and be reset every day.
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
the CI builds. NOTE: the VSTS agent is locked down so upgrade from the Azure console
will fail. Expectation is that the nodes are cycled daily and will pick up latest
Azure VSTS agent on rebuild.

# Machine initialization

Machine initialization is done based on the local checkout of the repo on each
macOS host. This means that changes to the macOS nodes init script need to be
propagated manually at this time. PRs that change macOS nodes configs should
include an audit trail of deploying those changes in their comments.

# Wiping nodes on a cron

macOS nodes are wiped every day overnight (at different time each, and after
checking the machine is not processing any job, so as to minimize service
interruption).

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
