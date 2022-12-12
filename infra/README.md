# Daml

This is the terraform code used by the Daml repository to deploy supporting
infrastructure such as the Bazel caches, Nix caches and Azure Pipeline (VSTS)
Agents.

## Setup

To deploy the infrastructure changes, you will to get access to the
`da-dev-gcp-daml-language` Google project from DA IT. Then run
`gcloud auth login` to configure the local credentials.

## Deployment

All the infrastructure is currently deployed using
[Terraform](https://www.terraform.io). For convenience we have a little
wrapper script that you can run to apply the latest changes:

```
$ ./apply
```

### Zero-downtime deployments

Changing the template of an instance manager tends to kill all the machines in
the current group. Therefore, we're experimenting with keeping two groups per
logical group. (This is currently done only for the Hoogle servers.)

> You can follow along with an example of such a deployment in the
> commit-by-commit view of [#9362](https://github.com/digital-asset/daml/pull/9362/commits).

The default state should be to split machines between the two groups evenly,
and have the exact same configuration in each group. When a change needs to be
made, one can set the size of one of the groups to 0, make the appropriate
changes, then ramp up the size of the "changed" group. Once enough nodes in the
new group have been started and are confirmed to work, the old group can be
resized to 0 (then updated to match so we don't accidentally roll back later).

For example, if we were to change the way in which the hoogle servers
initiailize, we could:

1. Change the size of the green group to 0 and bump the blue group to 3.
2. Add a new field to both groups containing the lines we want to change, and
   splice those lines in the init script. At this point a `terraform plan`
   should report no change.
3. Change the value for the green group and bump its size to 1. Deploy, wait
   for the green machine to start.
4. If everything went well, bump the size of the green group to 2 and zero the
   size of the blue group.
5. Update the variables in the blue group to match the green group, and bump
   the size of the blue group to 1.

This way, we can do a rolling upgrade while controlling how many working
machines we have at all times, and thus perform zero-downtime upgrades with
hopefully no impact on users. (Assuming the only failure mode of the new
machines is to fail at startup.)

> For CI nodes, it's better to do a whole group at once, with overlap: first
> spin up the second group with the new settings, wait for the nodes to be
> available, deactivate the old nodes in the Azure UI, and when all of the old
> nodes are idle, set the old group size to 0. This is because when changing a
> group's size we do not control which node gets killed, and we'd rather not
> kill a node that is currently running a job.
>
> Alternatively, killing a node in an instance group through the [GCP Console
> UI](https://console.cloud.google.com) will both kill that node _and_ reduce
> the instance group count by one, so that can be used to manually reduce a
> group's size one machine at a time if we don't want to wait for all the nodes
> in a group to be idle.

At the Terraform level, this is achieved by using the special `count` field,
which allows one to duplicate (multiplicate) a Terraform resource description.
Within a resource that has a `count` entry, the special variable `count.index`
will be the index of the "current" iteration of a resource. When other
resources reference one that has a `count` property, they have to address is as
an array. For example, if we have a resouce defined by:

```terraform
resource text_field my_counter {
  count = 2
  value = count.index
}
```

we would create two `text_field` resources both called `my_counter`. They could
be accessed with `text_field.my_counter[0]` and `text_field.my_counter[1]`. If
we wanted to create two corresponding resources we could write something like:

```terraform
resource derived_text_field my_derivation {
  count = length(text_field.my_counter)
  value = "the count is ${text_field.my_counter[count.index]}"
}
```

A word of warning: because of the way in which Terraform keeps track of state,
if you swapped two such resources, Terraform will destroy and recreate both. In
other words, it keeps track of each indivividual element in the list, not of
the list itself. Similarly, if you deleted an element in the list, all elements
after it would be destroyed and recreated.

## Writer service-account key

To avoid holding the secret key into the store, creating the key has to be
done through the UI.

This can be done here: https://console.cloud.google.com/iam-admin/serviceaccounts/details/104272946446260011088?project=da-dev-gcp-daml-language

## Setting up credentials

In order to interact with these Terraform files, you will need security to give
you access to the relevant GCP project (`da-dev-gcp-daml-language`), and login
via `gcloud` by running:

```bash
gcloud auth application-default login --account your.name@gcloud-domain.com
```

## Resetting build nodes

Permissions to reset build nodes are defined in `periodic-killer.tf` using
the `killCiNodes` role. CI nodes are managed so killed nodes will be
immediately replaced by a new one with the exact same configuration (but
starting its initialization from scratch); we can therefore see killing a
node and resetting a node as the same operation.

Nodes can be listed with
```
gcloud compute instances list --project=da-dev-gcp-daml-language
```

and individual nodes can be killed with
```
gcloud compute instances --project=da-dev-gcp-daml-language delete --zone=us-east4-a vsts-agent-linux-dhw4
```

where zone and name have to match.

As a reference, here are a couple `zsh` functions I have added to my shell to
make my life easier:
```zsh
refresh_machines() {
    machines=$(gcloud compute instances list --format=json --project=da-dev-gcp-daml-language | jq -c '[.[] | select (.name | startswith("vsts-")) | {key: .name, value: .zone | sub (".*/"; "")}] | from_entries')
}

kill_machine() {
    if [ -z "$machines" ]; then
        refresh_machines
    fi
    for machine in $@; do
        gcloud -q compute instances --project=da-dev-gcp-daml-language delete --zone=$(echo $machines | jq -r ".[\"$machine\"]") $machine
    done
}
_kill_machine() {
    local machine_names
    if [ -z "$machines" ]; then
        refresh_machines
    fi
    machine_names=$(echo $machines | jq -r "keys - $(echo -n $words | jq -sRc 'split(" ")') | .[]")
    _arguments "*: :($machine_names)"
}
compdef _kill_machine kill_machine
```

Nodes should be deactivated and idle before they are killed, to avoid killing a
running job.

## Deactivating a node

Deactivating a node can be done from the [Azure Pipelines
UI](https://dev.azure.com) in [Organization Settings / Agent
Pools](https://dev.azure.com/digitalasset/daml/_settings/agentqueues), for
people with the appropriate access rights. That page also allows one to see how
many pending jobs there are and, for each machine, which build they are
currently running.

On the Agents tab of a pool's page, there is an Enabled column with a toggle.
Clicking that toggle deactivates a node: it stays in the pool and is still
running, but will not get assigned new jobs. This is a good step towards
killing the node while still allowing it to finish the job it is currently
processing (or making sur it doesn't get assigned another job in-between you
looking at the node state and killing it).

As there is no way to tell Azure to run a job on a specific node, deactiating a
node is the only way to ensure a job gets assigned to "another one" if that
node is suspected of being corrupted in some way.

## Wiping the Windows Bazel cache

The Windows Bazel cache tends to get corrupted. Over time, we have tried to
address various ways in which it gets corrupted through patches to Bazel (that
have since been upstreamed) and a periodic clean-up job (`fix_bazel_cache`) as
part of the CI build.

Though rare, there are still cases where the cache gets corrupted, either
locally on a machine or globally in the GCS bucket.

When a machine's cache is suspected corrupt, it is best to just kill the node.

If killing a node doesn't solve the issue (i.e. the cache appears corrupt even
on "new" nodes running their first job), it means the network cache is corrupt.
Fortunately, the Windows network cache is segmented across builds: there is a
separate cache prefix for the PRs build, the main branch build, the
compatibility run, etc.

This means that corruption is limited to one build, limiting the impact to some
extent, and that we can wipe only the relevant cache, leaving the rest intact.

Wiping the cache amounts to running a command along the lines of:

```
gsutil -m rm -r gs://daml-bazel-cache/3ecf0dac8af5-v13/
```

You need delete access on the `daml-bazel-cache` GCS bucket for this to work.

The name of the cache (in this example `3ecf0dac8af5-v13`) can be derived from
the version number in `ci/configure-bazel.sh` when setting the suffix; the hash
is printed by the `Configure Bazel` step, and needs to be taken from the output
of an affected failed build.

Note that, due to the sheer number of files, wiping the cache for the PRs build
can take upwards of 15 hours, during which the machine on which the command is
run must be turned on and connected to the internet.
