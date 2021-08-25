# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_network" "es" {
  name = "es-network"
}

locals {
  es_ssh  = 0
  es_feed = 1
  es_clusters = [
    {
      suffix         = "-blue",
      ubuntu_version = "2004",
      size           = 5,
      init           = "[]",
      type           = "n2-highmem-2",
      xmx            = "12g",
    },
    {
      suffix         = "-green",
      ubuntu_version = "2004",
      size           = 0,
      init           = "[]",
      type           = "e2-standard-2",
      xmx            = "6g",
    },
    {
      suffix         = "-init",
      ubuntu_version = "2004",
      size           = 0,
      init           = "[\"$(hostname)\"]",
      type           = "e2-standard-2",
      xmx            = "6g",
    },
  ]

  es_ports = [
    { name = "es", port = "9200" },
    { name = "kibana", port = "5601" },
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
    ports    = ["22"]
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
    ports    = ["5601", "9200"]
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
    disk_size_gb = 200
    source_image = "ubuntu-os-cloud/ubuntu-${local.es_clusters[count.index].ubuntu_version}-lts"
  }

  metadata_startup_script = <<STARTUP
#! /bin/bash
set -euo pipefail
apt-get update
apt-get -y upgrade
### stackdriver
curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash

## Install Docker
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

docker build -t es .
docker run -d \
           --name es \
           -p 9200:9200 \
           -p 9300:9300 \
           -e ES_JAVA_OPTS="-Xmx${local.es_clusters[count.index].xmx} -Xms${local.es_clusters[count.index].xmx}" \
           es

docker run -d \
           --name kibana \
           -p 5601:5601 \
           --link es:elasticsearch \
           -e TELEMETRY_ENABLED=false \
           docker.elastic.co/kibana/kibana:7.13.2

## Getting container output directly to the GCP console

( exec 1> >(while IFS= read -r line; do echo "elastic: $line"; done); docker logs -f es ) &
( exec 1> >(while IFS= read -r line; do echo "kibana: $line"; done); docker logs -f kibana ) &

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

output "es_address" {
  value = google_compute_address.es[0].address
}

output "kibana_address" {
  value = google_compute_address.es[1].address
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
     --arg index "$(index "$job" events)" \
     --arg job "$job" \
     < "$file" \
     '
     { index: { _index: $index, _id: ($job + "-" + $cmd + "-events-" + (input_line_number | tostring)) } },
     { job: $job_md[0],
       command: { name: $cmd },
       buildEvent: .
     }
     '
  jq -c \
     --slurpfile job_md "$job/job-md.json" \
     --arg cmd "$cmd" \
     --arg index "$(index "$job" jobs)" \
     --arg job "$job" \
     < "$file" \
     --slurp \
     '
     { index: { _index: $index, _id: ($job + "-" + $cmd + "-events") } },
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
     --arg index "$(index "$job" events)" \
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
  jq -c \
     --slurpfile job_md "$job/job-md.json" \
     --arg cmd "$cmd" \
     --arg index "$(index "$job" jobs)" \
     --arg job "$job" \
     < "$file" \
     '
     { index: { _index: $index, _id: ($job + "-" + $cmd + "-profile") } },
     { job: $job_md[0],
       command: { name: $cmd },
       traceEvent: .traceEvents
     }
     '
}

bulk_upload() {
  local res
  res="$(curl -X POST "http://$ES_IP/_bulk?filter_path=errors,items.*.status" \
              -H 'Content-Type: application/json' \
              --fail \
              -s \
              --data-binary @- \
              | jq -r '.items[].index.status' | sort | uniq -c)"
  echo "$res"
}

patch() {
  local job map
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
    sed -i "$sed_command" "$job"/{build-events,build-profile,test-events,test-profile}.json
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
  prefix="$2"
  echo "$prefix-$(echo $job | cut -c1-10)"
}

pid=$$
exec 2> >(while IFS= read -r line; do echo "$(date -uIs) [$pid] [err]: $line"; done)
exec 1> >(while IFS= read -r line; do echo "$(date -uIs) [$pid] [out]: $line"; done)

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
    ensure_index "$job" "$(index "$job" jobs)"
    ensure_index "$job" "$(index "$job" events)"
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

chmod +x /root/cron.sh

ES_IP=${google_compute_address.es[0].address}

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
      "storage-ro",
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
