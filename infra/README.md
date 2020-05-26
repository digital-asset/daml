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
