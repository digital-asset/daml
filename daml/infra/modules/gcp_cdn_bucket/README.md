# A Google Storage Bucket + CDN configuration

This modules contains essentially two things:

* A GCS bucket to store objects into
* A load-balancer connected to it

It also makes a few assumptions:

* A service account will be created to write into the bucket
* All objects are meant to be publicly-readable

## Module config

`> terraform-docs md .`
<!-- BEGIN mdsh -->
## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| cache\_retention\_days | The number of days to keep the objects around | string | n/a | yes |
| labels | Labels to apply on all the resources | map | `<map>` | no |
| name | Name prefix for all the resources | string | n/a | yes |
| project | GCP project name | string | n/a | yes |
| region | GCP region in which to create the resources | string | n/a | yes |
| ssl\_certificate | A reference to the SSL certificate, google managed or not | string | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| bucket\_name | Name of the GCS bucket that will receive the objects. |
| external\_ip | The external IP assigned to the global fowarding rule. |

<!-- END mdsh -->
