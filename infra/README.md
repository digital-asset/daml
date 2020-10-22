# DAML

This is the terraform code used by the DAML repository to deploy supporting
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
