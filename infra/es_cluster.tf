# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_network" "es" {
  name = "es-network"
}

/*
Instruct ES to move all data out of blue nodes:
PUT _cluster/settings
{
  "transient" : {
    "cluster.routing.allocation.exclude._ip" : "es-blue-*"
  }
}
use null to reset
*/

locals {
  es_ssh  = 1
  es_feed = 0
  es_clusters = [
    {
      suffix         = "-blue",
      ubuntu_version = "2004",
      size           = 0,
      init           = "[]",
      type           = "n2-highmem-2",
      xmx            = "12g",
      disk_size      = 300,
    },
    {
      suffix         = "-green",
      ubuntu_version = "2004",
      size           = 5,
      init           = "[]",
      type           = "n2-highmem-2",
      xmx            = "12g",
      disk_size      = 500,
    },
    {
      suffix         = "-init",
      ubuntu_version = "2004",
      size           = 0,
      init           = "[\"$(hostname)\"]",
      type           = "e2-standard-2",
      xmx            = "6g",
      disk_size      = 200,
    },
  ]

  es_ports = [
    { name = "es", port = "9200" },
    { name = "kibana", port = "5601" },
    //{ name = "cerebro", port = "9000" },
  ]
}

resource "google_compute_firewall" "es-ssh" {
  ## Disabled by default
  count   = local.es_ssh
  name    = "es-ssh"
  network = google_compute_network.es.name
  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
  allow {
    protocol = "tcp"
    ports    = ["22", "9000"]
  }
  source_ranges = [      # VPNs
    "35.194.81.56/32",   # North Virginia
    "35.189.40.124/32",  # Sydney
    "35.198.147.95/32",  # Frankfurt
    "18.210.210.130/32", # consultant
  ]
}

resource "google_compute_firewall" "es-http" {
  name        = "es-http"
  network     = google_compute_network.es.name
  target_tags = ["es"]

  source_ranges = [
    ## Google Load Balancer
    "130.211.0.0/22",
    "35.191.0.0/16",
  ]

  allow {
    protocol = "tcp"
    ports    = [for p in local.es_ports : p.port]
  }
}

resource "google_compute_firewall" "es-internal" {
  name        = "es-internal"
  network     = google_compute_network.es.name
  target_tags = ["es"]

  source_ranges = [
    ## Internal
    "10.128.0.0/9",
  ]

  allow {
    protocol = "tcp"
    ports    = ["9300"]
  }
}

resource "google_service_account" "es-discovery" {
  # account_id allows - but not _
  account_id   = "es-discovery"
  display_name = "es-discovery"
}

resource "google_project_iam_custom_role" "es-discovery" {
  # role_id allows _ but not -
  role_id     = "es_discovery"
  title       = "es-discovery"
  description = "es-discovery"
  permissions = [
    # Cloud logging
    "logging.logEntries.create",
    # ES discovery
    "compute.instances.get",
    "compute.instances.list",
  ]
}

resource "google_project_iam_member" "es-discovery" {
  project = local.project
  role    = google_project_iam_custom_role.es-discovery.id
  member  = "serviceAccount:${google_service_account.es-discovery.email}"
}

resource "google_compute_instance_template" "es" {
  count        = length(local.es_clusters)
  name_prefix  = "es${local.es_clusters[count.index].suffix}-"
  machine_type = local.es_clusters[count.index].type
  tags         = ["es"]
  labels       = local.machine-labels

  disk {
    boot         = true
    disk_size_gb = local.es_clusters[count.index].disk_size
    source_image = "ubuntu-os-cloud/ubuntu-${local.es_clusters[count.index].ubuntu_version}-lts"
  }

  metadata_startup_script = <<STARTUP
#! /bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y upgrade
### stackdriver
curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash

## Install Docker

mkdir -p /etc/docker
cat <<CONFIG > /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {"max-size": "10m", "max-file": "3"}
}
CONFIG

apt-get install -y \
  apt-transport-https \
  ca-certificates \
  gnupg \
  lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io

## Set up ES

sysctl -w vm.max_map_count=262144

mkdir -p /root/es-docker
cd /root/es-docker

cat <<EOF > es.yml
cluster.name: es
node.name: $(hostname)
cluster.initial_master_nodes: ${local.es_clusters[count.index].init}
discovery.seed_providers: gce
discovery.gce.tags: es
cloud.gce.project_id: ${local.project}
cloud.gce.zone: ${local.zone}
network.host: 0.0.0.0
network.publish_host: _gce_
http.max_content_length: 500mb
EOF

cat <<EOF > Dockerfile
FROM docker.elastic.co/elasticsearch/elasticsearch:7.13.2

RUN bin/elasticsearch-plugin install --batch discovery-gce
COPY es.yml /usr/share/elasticsearch/config/elasticsearch.yml
EOF

mkdir -p /root/es-data
chown 1000:0 /root/es-data

docker build -t es .
docker run -d \
           --restart on-failure \
           --name es \
           -p 9200:9200 \
           -p 9300:9300 \
           -e ES_JAVA_OPTS="-Xmx${local.es_clusters[count.index].xmx} -Xms${local.es_clusters[count.index].xmx}" \
           -v /root/es-data:/usr/share/elasticsearch/data \
           es

docker run -d \
           --restart on-failure \
           --name kibana \
           -p 5601:5601 \
           --link es:elasticsearch \
           -e TELEMETRY_ENABLED=false \
           docker.elastic.co/kibana/kibana:7.13.2

docker run -d \
           -p 9000:9000 \
           --link es \
           --name cerebro \
           lmenezes/cerebro:0.9.4

## Getting container output directly to the GCP console

( exec 1> >(while IFS= read -r line; do echo "elastic: $line"; done); docker logs -f es ) &
( exec 1> >(while IFS= read -r line; do echo "kibana: $line"; done); docker logs -f kibana ) &
( exec 1> >(while IFS= read -r line; do echo "cerebro: $line"; done); docker logs -f cerebro ) &

for job in $(jobs -p); do
    wait $job
done

STARTUP



  network_interface {
    network = google_compute_network.es.name
    access_config {}
  }

  service_account {
    email = google_service_account.es-discovery.email
    scopes = [
      # Required for cloud logging
      "logging-write",
      # Required per ES documentation
      "compute-rw",
    ]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_instance_group_manager" "es" {
  provider           = google-beta
  count              = length(local.es_clusters)
  name               = "es${local.es_clusters[count.index].suffix}"
  base_instance_name = "es${local.es_clusters[count.index].suffix}"
  zone               = local.zone
  target_size        = local.es_clusters[count.index].size

  version {
    name              = "es${local.es_clusters[count.index].suffix}"
    instance_template = google_compute_instance_template.es[count.index].self_link
  }

  dynamic named_port {
    for_each = local.es_ports
    content {
      name = named_port.value["name"]
      port = named_port.value["port"]
    }
  }

  update_policy {
    type                    = "PROACTIVE"
    minimal_action          = "REPLACE"
    max_unavailable_percent = 100
  }
}

resource "google_compute_address" "es" {
  count        = length(local.es_ports)
  name         = "es-${local.es_ports[count.index].name}"
  network_tier = "STANDARD"
}

resource "google_compute_health_check" "es-http" {
  count              = length(local.es_ports)
  name               = "es-http-${local.es_ports[count.index].name}"
  check_interval_sec = 10
  timeout_sec        = 1

  tcp_health_check {
    port = local.es_ports[count.index].port
  }
}

resource "google_compute_backend_service" "es-http" {
  count                 = length(local.es_ports)
  name                  = "es-http-${local.es_ports[count.index].name}"
  health_checks         = [google_compute_health_check.es-http[count.index].self_link]
  port_name             = local.es_ports[count.index].name
  security_policy       = google_compute_security_policy.es.self_link
  load_balancing_scheme = "EXTERNAL"

  dynamic backend {
    for_each = local.es_clusters
    content {
      group           = google_compute_instance_group_manager.es[backend.key].instance_group
      balancing_mode  = "UTILIZATION"
      capacity_scaler = 1
    }
  }
}

resource "google_compute_url_map" "es-http" {
  count           = length(local.es_ports)
  name            = "es-http-${local.es_ports[count.index].name}"
  default_service = google_compute_backend_service.es-http[count.index].self_link
}

resource "google_compute_target_http_proxy" "es-http" {
  count   = length(local.es_ports)
  name    = "es-http-${local.es_ports[count.index].name}"
  url_map = google_compute_url_map.es-http[count.index].self_link
}

resource "google_compute_forwarding_rule" "es-http" {
  count        = length(local.es_ports)
  name         = "es-http-${local.es_ports[count.index].name}"
  target       = google_compute_target_http_proxy.es-http[count.index].self_link
  ip_address   = google_compute_address.es[count.index].address
  port_range   = "80"
  network_tier = "STANDARD"
}

## The proxy implied by the forwarding rule sits outside our network, but we
## still want to limit to VPNs.
resource "google_compute_security_policy" "es" {
  name = "es"

  rule {
    action   = "deny(403)"
    priority = "2147483647"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["*"]
      }
    }
    description = "Default: deny all"
  }

  rule {
    action   = "allow"
    priority = "1000"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = [      # VPNs
          "35.194.81.56/32",   # North Virginia
          "35.189.40.124/32",  # Sydney
          "35.198.147.95/32",  # Frankfurt
          "18.210.210.130/32", # consultant
          "${google_compute_address.es-feed.address}/32"
        ]
      }
    }
    description = "Allow VPNs"
  }
}

output "es_addresses" {
  value = { for idx, p in local.es_ports : p.name => google_compute_address.es[idx].address }
}

resource "google_compute_address" "es-feed" {
  name = "es-feed"
}

resource "google_service_account" "es-feed" {
  account_id   = "es-feed"
  display_name = "es-feed"
}

resource "google_project_iam_custom_role" "es-feed" {
  role_id     = "es_feed"
  title       = "es-feed"
  description = "es-feed"
  permissions = [
    # Cloud logging
    "logging.logEntries.create",
    # Access GCS bucket
    "storage.objects.get",
    "storage.objects.list",
  ]
}

resource "google_project_iam_member" "es-feed" {
  project = local.project
  role    = google_project_iam_custom_role.es-feed.id
  member  = "serviceAccount:${google_service_account.es-feed.email}"
}

resource "google_project_iam_custom_role" "es-feed-write" {
  role_id     = "es_feed_write"
  title       = "es-feed-write"
  description = "es-feed-write"
  permissions = [
    "storage.objects.create"
  ]
}

resource "google_project_iam_member" "es-feed-write" {
  project = local.project
  role    = google_project_iam_custom_role.es-feed-write.id
  member  = "serviceAccount:${google_service_account.es-feed.email}"

  condition {
    title       = "es_feed_write"
    description = "es_feed_write"
    expression  = "resource.name.startsWith(\"projects/_/buckets/${google_storage_bucket.data.name}/objects/kibana-export\")"
  }
}

resource "google_compute_instance_group_manager" "es-feed" {
  provider           = google-beta
  name               = "es-feed"
  base_instance_name = "es-feed"
  zone               = local.zone
  target_size        = local.es_feed

  version {
    name              = "es-feed"
    instance_template = google_compute_instance_template.es-feed.self_link
  }

  update_policy {
    type                    = "PROACTIVE"
    minimal_action          = "REPLACE"
    max_unavailable_percent = 100
  }
}

resource "google_compute_instance_template" "es-feed" {
  name_prefix  = "es-feed"
  machine_type = "e2-standard-2"
  tags         = ["es"]
  labels       = local.machine-labels

  disk {
    boot         = true
    source_image = "ubuntu-os-cloud/ubuntu-2004-lts"
    disk_size_gb = "200"
  }

  metadata_startup_script = <<STARTUP
apt-get update
export DEBIAN_FRONTEND=noninteractive
apt-get upgrade -y
apt-get install -y jq

### stackdriver
curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash

cat <<'CRON' >/root/cron.sh
#!/usr/bin/env bash
set -euo pipefail

emit_mappings() {
  jq -nc '{
      mappings: {
        properties: {
          job: {
            properties: {
              timestamp: { type: "date" },
              id: { type: "keyword" },
              agent_id: { type: "keyword" },
              agent_job_name: { type: "keyword" },
              agent_machine_name: { type: "keyword" },
              agent_name: { type: "keyword" },
              agent_os: { type: "keyword" },
              agent_os_architecture: { type: "keyword" },
              build_build_id: { type: "keyword" },
              build_build_number: { type: "keyword" },
              build_definition_name: { type: "keyword" },
              build_source_branch: { type: "keyword" },
              build_source_branch_name: { type: "keyword" },
              build_source_version: { type: "keyword" },
              system_job_attempt: { type: "keyword" },
              system_job_display_name: { type: "keyword" },
              system_job_id: { type: "keyword" },
              system_job_name: { type: "keyword" },
              system_pullRequest_pullRequestId: { type: "keyword" },
              system_pullRequest_pullRequestNumber: { type: "keyword" },
              system_pullRequest_mergedAt: { type: "keyword" },
              system_pullRequest_sourceBranch: { type: "keyword" },
              system_pullRequest_targetBranch: { type: "keyword" },
              system_pullRequest_sourceRepositoryUri: { type: "keyword" },
              system_pullRequest_sourceCommitId: { type: "keyword" },
              git_branch_sha: { type: "keyword" },
              git_main_sha: { type: "keyword" },
              git_fork_point: { type: "keyword" },
              git_current_branch: { type: "keyword" },
              git_current_commit: { type: "keyword" },
              git_current_tree: { type: "keyword" },
            }
          },
          command: {
            properties: {
              name: { type: "keyword" }
            }
          },
          buildEvent: {
            properties: {
              id: { type: "object" },
              children: { type: "nested" },
              lastMessage: { type: "boolean" },
              progress: {
                properties: {
                  stdout: { type: "text" },
                  stderr: { type: "text" },
                }
              },
              aborted: {
                properties: {
                  reason: { type: "keyword" },
                  description: { type: "text" },
                }
              },
              started: {
                properties: {
                  uuid: { type: "keyword" },
                  startTimeMillis: {
                    type: "date",
                    format: "epoch_millis"
                  },
                  buildToolVersion: { type: "keyword" },
                  optionsDescription: { type: "text" },
                  command: { type: "keyword" },
                  workingDirectory: { type: "keyword" },
                  workspaceDirectory: { type: "keyword" },
                  serverPid: { type: "keyword" },
                }
              },
              unstructuredCommandLine: {
                properties: {
                  args: { type: "keyword" },
                }
              },
              structuredCommandLine: {
                properties: {
                  sections: { type: "nested" },
                },
              },
              optionsParsed: { type: "object" },
              workspaceStatus: {
                properties: {
                  item: {
                    type: "nested",
                    properties: {
                      key: { type: "keyword" },
                      value: { type: "text" },
                    },
                  },
                },
              },
              fetch: { type: "object" },
              configuration: { type: "object" },
              expanded: { type: "object" },
              configured: { type: "object" },
              action: {
                properties: {
                  actionMetadataLogs: { type: "nested" },
                },
              },
              namedSetOfFiles: { type: "object" },
              completed: {
                properties: {
                  success: { type: "boolean" },
                  outputGroup: { type: "nested" },
                  importantOutput: { type: "nested" },
                  directoryOutput: { type: "nested" },
                  testTimeoutSeconds: { type: "long" },
                },
              },
              testResult: {
                properties: {
                  cachedLocally: { type: "boolean" },
                  testAttemptStartMillisEpoch: {
                    type: "date",
                    format: "epoch_millis",
                  },
                  testAttemptDurationMillis: { type: "long" },
                  testActionOutput: { type: "nested" },
                  executionInfo: {
                    properties: {
                      timeoutSeconds: { type: "integer" },
                      cachedRemotely: { type: "boolean" },
                      exitCode: { type: "integer" },
                      timingBreakdown: { type: "nested" },
                      resourceUsage: { type: "nested" },
                    },
                  },
                },
              },
              testSummary: {
                properties: {
                  overallStatus: { type: "keyword" },
                  totalRunCount: { type: "integer" },
                  runCount: { type: "integer" },
                  shardCount: { type: "integer" },
                  passed: { type: "nested" },
                  failed: { type: "nested" },
                  totalNumCached: { type: "integer" },
                  firstStartTimeMillis: {
                    type: "date",
                    format: "epoch_millis",
                  },
                  lastStopTimeMillis: {
                    type: "date",
                    format: "epoch_millis",
                  },
                  totalRunDurationMillis: { type: "long" },
                },
              },
              finished: {
                properties: {
                  overallSuccess: { type: "boolean" },
                  exitCode: {
                    properties: {
                      name: { type: "keyword" },
                      code: { type: "integer" },
                    },
                  },
                  finishTimeMillis: {
                    type: "date",
                    format: "epoch_millis",
                  },
                  anomalyReport: {
                    properties: {
                      wasSuspended: { type: "boolean" },
                    },
                  },
                },
              },
              buildToolLogs: {
                properties: {
                  log: { type: "nested" },
                },
              },
              buildMetrics: {
                properties: {
                  actionSummary: {
                    properties: {
                      actionsCreated: { type: "long" },
                      actionsExecuted: { type: "long" },
                    },
                  },
                  memoryMetrics: {
                    properties: {
                      usedHeapSizePostBuild: { type: "long" },
                      peakPostGcHeapSize: { type: "long" },
                    },
                  },
                  targetMetrics: {
                    properties: {
                      targetsLoaded: { type: "long" },
                      targetsConfigured: { type: "long" },
                    },
                  },
                  packageMetrics: {
                    properties: {
                      packagesLoaded: { type: "long" },
                    },
                  },
                  timingMetrics: {
                    properties: {
                      cpuTimeInMs: { type: "long" },
                      wallTimeInMs: { type: "long" },
                    },
                  },
                },
              },
              workspaceConfig: { type: "object" },
              buildMetadata: { type: "object" },
              convenienceSymlinksIdentified: {
                properties: {
                  convenienceSymlinks: { type: "nested" },
                },
              },
            }
          },
          traceEvent: {
            properties: {
              cat: { type: "keyword" },
              name: { type: "keyword" },
              ph: { type: "keyword" },
              pid: { type: "integer" },
              tid: { type: "integer" },
              args: { type: "object" },
              ts: { type: "long" },
              dur: { type: "long" },
              args: {
                properties: {
                  name: { type: "keyword" },
                  target: { type: "keyword" },
                },
              },
            },
          },
        }
      },
      settings: {
        number_of_replicas: 1,
        number_of_shards: 3,
        "mapping.nested_objects.limit": 100000
      }
    }'
}

ensure_index() {
  local job index
  job="$1"
  index="$2"
  if ! [ -f $DONE/$index ]; then
    if curl -s --fail -I http://$ES_IP/$index >/dev/null; then
      echo "$job: index $index already exists"
    else
      echo "$job: creating index $index"
      emit_mappings | curl -XPUT http://$ES_IP/$index \
                           -s \
                           -H 'Content-Type: application/json' \
                           --fail \
                           --data-binary @- >/dev/null
    fi
    touch $DONE/$index
  fi
}

emit_build_events() {
  local job cmd file
  job="$1"
  cmd="$2"
  file="$3"
  jq -c \
     --slurpfile job_md "$job/job-md.json" \
     --arg cmd "$cmd" \
     --arg index "$(index "$job")" \
     --arg job "$job" \
     < "$file" \
     '
     { index: { _index: $index, _id: ($job + "-" + $cmd + "-events-" + (input_line_number | tostring)) } },
     { job: $job_md[0],
       command: { name: $cmd },
       buildEvent: .
     }
     '
}

emit_trace_events() {
  local job cmd index file
  job="$1"
  cmd="$2"
  file="$3"
  jq -c \
     --slurpfile job_md "$job/job-md.json" \
     --arg cmd "$cmd" \
     --arg index "$(index "$job")" \
     --arg job "$job" \
     < "$file" \
     '
     .traceEvents
     | to_entries[]
     | { index: { _index: $index, _id: ($job + "-" + $cmd + "-profile-" + (.key | tostring)) } },
       { job: $job_md[0],
         command: { name: $cmd },
         traceEvent: .value
       }
     '
}

bulk_upload() (

  ## Uploads a bunch of JSON objects, subject to these constraints:
  ##
  ## 1. The input file has one JSON object per line. We cannot bbreak lines, as
  ##    that would result in incomplete JSON objects.
  ## 2. JSON objects go in pairs: the first line is metadata for how ES should
  ##    ingest the second line. So we can't split in the middle of a pair
  ##    either.
  ## 3. The maximum size for a single upload is 500mb (set in the ES
  ##    configuration a bit higher in this file), so if a file is larger than
  ##    that we need to split it, respecting constraints 1 and 2.
  ##
  ## Because this function is defined with () rather than the usual {}, it runs
  ## in a subshell and can define its own scoped inner functions, as well as
  ## its own traps. Also, all variables are local.

  tmp=$(mktemp)
  chunk=$(mktemp)
  trap 'rm -f $tmp $chunk' EXIT
  cat - > $tmp
  lines_to_process=$(wc -l $tmp | awk '{print $1'})
  processed_lines=0
  lines_per_chunk=$lines_to_process

  push_chunk() {
    curl -X POST "http://$ES_IP/_bulk?filter_path=errors,items.*.status" \
         -H 'Content-Type: application/json' \
         --fail \
         -s \
         --data-binary @$chunk \
         | jq -r '.items[].index.status' | sort | uniq -c
    processed_lines=$(( processed_lines + lines_per_chunk ))
    lines_per_chunk=$(( lines_per_chunk * 2 ))
  }

  get_next_chunk() {
    (
    # tail -n +N drops the first N-1 lines
    # tail is expected to fail with 141 (pipe closed) on intermediate
    # iterations
    tail -n +$(( processed_lines + 1)) $tmp || (( $? == 141))
    ) \
      | head -n $lines_per_chunk \
      > $chunk
  }

  all_lines_have_been_processed() (( processed_lines >= lines_to_process ))

  # limit chunk size to 50MB
  # This will fail dramatically if we ever have a single line over 50MB
  chunk_is_too_big() (( $(du $chunk | awk '{print $1}') > 50000 ))

  reduce_chunk_size() {
    # divide by two, but keep an even number
    lines_per_chunk=$(( lines_per_chunk / 4 * 2))
  }

  until all_lines_have_been_processed; do
    get_next_chunk
    if chunk_is_too_big; then
      reduce_chunk_size
    else
      push_chunk
    fi
  done
)

patch() {
  local job map file
  job="$1"
  # Replace shortened Scala test names by their long names.
  # See //bazel_tools:scala.bzl%da_scala_test_short_name_aspect.
  map="scala-test-suite-name-map.json"
  if ! [[ -f "$job/$map" ]]; then
    echo "$job: no $map"
  else
    echo "$job: applying $map"
    # Generates a sed command to replace short labels by long labels.
    jq_command='to_entries | map("s|\(.key)\\b|\(.value)|g") | join(";")'
    sed_command="$(jq -r "$jq_command" <"$job/$map")"
    for f in build-events build-profile test-events test-profile; do
      file="$job/$f.json"
      if [ -f "$file" ]; then
        sed -i "$sed_command" "$file"
      fi
    done
  fi
}

push() {
  local job f pids
  job="$1"
  pids=""
  for cmd in "build" "test"; do

    f="$job/$cmd-events.json"
    if ! [[ -f "$f" ]]; then
      echo "$job: no $cmd-events.json"
    elif ! jq . >/dev/null 2>&1 < $f; then
      echo "$job: $cmd-events.json exists but is not valid json, skipping"
    else
      echo "$job: pushing $cmd-events.json"
      (emit_build_events "$job" "$cmd" "$f" | bulk_upload) &
      pids="$pids $!"
    fi

    f="$job/$cmd-profile.json"
    if ! [[ -f "$f" ]]; then
      echo "$job: no $cmd-profile.json"
    elif ! jq . >/dev/null 2>&1 < $f; then
      echo "$job: $cmd-profile.json exists but is not valid json, skipping"
    else
      echo "$job: pushing $cmd-profile.json"
      (emit_trace_events "$job" "$cmd" "$f" | bulk_upload) &
      pids="$pids $!"
    fi
  done
  for pid in $pids; do
    wait $pid
  done
}

index() {
  local job prefix
  job="$1"
  echo "events-$(echo $job | cut -c1-10)"
}

pid=$$
exec 2> >(while IFS= read -r line; do echo "$(date -uIs) [ingest] [$pid] [err]: $line"; done)
exec 1> >(while IFS= read -r line; do echo "$(date -uIs) [ingest] [$pid] [out]: $line"; done)

LOCK=/root/lock

if [ -f $LOCK ]; then
  echo "Already running; skipping."
  exit 0
else
  touch $LOCK
  trap "rm $LOCK; echo exited" EXIT
  echo "Starting..."
fi

echo "Running rsync..."
$GSUTIL -q -m rsync -r gs://daml-data/bazel-metrics/ $DATA/
echo "Total data size: $(du -hs $DATA | awk '{print $1}')."

todo=$(find $DATA -type f -name \*.tar.gz | sort)
comm=$(comm -23 <(for f in $todo; do basename $${f%.tar.gz}; done | sort) <(ls $DONE | sort))

echo "Need to push $(echo "$comm" | sed '/^$/d' | wc -l) files out of $(echo "$todo" | sed '/^$/d' | wc -l)."

for tar in $todo; do
  job=$(basename $${tar%.tar.gz})
  cd $(dirname $tar)
  if ! [ -f $DONE/$job ]; then
    ensure_index "$job" "$(index "$job")"
    tar --force-local -x -z -f "$(basename "$tar")"
    patch "$job"
    push "$job"
    rm -rf $job
    r=$(curl -H 'Content-Type: application/json' \
             --fail \
             -s \
             "http://$ES_IP/done/_doc/$job" \
             -d '{}')
    echo "$job: $(echo $r | jq '.result')"
    touch "$DONE/$job"
  fi
done
CRON

cat <<'HOURLY' >/root/hourly.sh
#!/usr/bin/env bash
set -euo pipefail

pid=$$
exec 2> >(while IFS= read -r line; do echo "$(date -uIs) [kibex] [$pid] [err]: $line"; done)
exec 1> >(while IFS= read -r line; do echo "$(date -uIs) [kibex] [$pid] [out]: $line"; done)

HOUR="$(date -u -Is | cut -c 1-13)"
TMP=$(mktemp)
TARGET="gs://daml-data/kibana-export/$HOUR.gz"

echo "Starting Kibana export..."

# Kibana export API does not support wildcard, so we list all of the object
# types that exist as of Kibana 7.13.
curl http://$KIBANA_IP/api/saved_objects/_export \
     -XPOST \
     -d'{"excludeExportDetails": true,
         "type": ["visualization", "dashboard", "search", "index-pattern",
                  "config", "timelion-sheet"]}' \
     -H 'kbn-xsrf: true' \
     -H 'Content-Type: application/json' \
     --fail \
     --silent \
     | gzip -9 > $TMP


echo "Pushing $TARGET"

$GSUTIL -q cp $TMP $TARGET

echo "Done."
HOURLY

chmod +x /root/cron.sh
chmod +x /root/hourly.sh

ES_IP=${google_compute_address.es[0].address}
KIB_IP=${google_compute_address.es[1].address}

DATA=/root/data
mkdir -p $DATA

DONE=/root/done
mkdir -p $DONE

echo "Synchronizing with cluster state..."
found=0
for prefix in jobs events; do
  for idx in $(curl --fail "http://$ES_IP/_cat/indices/$prefix-*?format=json" -s | jq -r '.[] | .index'); do
    found=$((found + 1))
    touch $DONE/$idx;
  done
done
echo "Found $found indices."

if curl -s --fail -I "http://$ES_IP/done" >/dev/null; then
  found=0
  res=$(curl --fail "http://$ES_IP/done/_search?_source=false&size=1000&scroll=5m" -s)
  while (echo $res | jq -e '.hits.hits != []' >/dev/null); do
    for id in $(echo $res | jq -r '.hits.hits[]._id'); do
      found=$((found + 1))
      touch $DONE/$id
    done
    scroll_id=$(echo $res | jq -r '._scroll_id')
    res=$(curl "http://$ES_IP/_search/scroll" \
               -s \
               --fail \
               -d "$(jq --arg id "$scroll_id" \
                        -n \
                        '{scroll: "5m", scroll_id: $id}')" \
               -H 'Content-Type: application/json')
  done
  echo "Found $found jobs."
else
  echo "No done index; creating..."
  r=$(curl -XPUT "http://$ES_IP/done" \
           -d '{"settings": {"number_of_replicas": 2}}' \
           --fail \
           -s \
           -H 'Content-Type: application/json')
  echo $r
fi

cat <<CRONTAB >> /etc/crontab
* * * * * root GSUTIL="$(which gsutil)" DONE="$DONE" DATA="$DATA" ES_IP="$ES_IP" /root/cron.sh >> /root/log 2>&1
1 * * * * root GSUTIL="$(which gsutil)" KIBANA_IP="$KIB_IP" /root/hourly.sh >> /root/log 2>&1
CRONTAB

echo "Waiting for first run..." > /root/log
tail -f /root/log

STARTUP

  network_interface {
    network = google_compute_network.es.name
    access_config {
      nat_ip = google_compute_address.es-feed.address
    }
  }

  service_account {
    email = google_service_account.es-feed.email
    scopes = [
      # Required for cloud logging
      "logging-write",
      # Read access to storage
      "storage-rw",
    ]
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = true
  }

  lifecycle {
    create_before_destroy = true
  }

}
