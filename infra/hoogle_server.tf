# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_network" "hoogle" {
  name = "hoogle-network"
}

resource "google_compute_firewall" "hoogle" {
  name        = "hoogle-firewall"
  network     = "${google_compute_network.hoogle.name}"
  target_tags = ["hoogle"]

  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]

  allow {
    protocol = "tcp"
    ports    = ["8080", "8081"]
  }
}

resource "google_compute_instance_template" "hoogle" {
  name_prefix  = "hoogle-"
  machine_type = "n1-standard-1"
  tags         = ["hoogle"]
  labels       = "${local.machine-labels}"

  disk {
    boot         = true
    disk_size_gb = 20
    source_image = "ubuntu-os-cloud/ubuntu-1604-lts"
  }

  metadata_startup_script = <<STARTUP
#! /bin/bash
set -euo pipefail
apt-get update
apt-get -y upgrade
### stackdriver
curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash
### nginx
apt-get -y install nginx
cat > /etc/nginx/nginx.conf <<NGINX
user www-data;
worker_processes auto;
pid /run/nginx.pid;
events {
  worker_connections 768;
}
http {
  sendfile on;
  tcp_nopush on;
  tcp_nodelay on;
  keepalive_timeout 65;
  types_hash_max_size 2048;
  include /etc/nginx/mime.types;
  default_type application/octet-stream;
  access_log /var/log/nginx/access.log;
  error_log /var/log/nginx/error.log;
  server {
    listen 8081 default_server;
    server_name _;
    return 307 https://hoogle.daml.com\$request_uri;
  }
}
NGINX
service nginx restart
### hoogle
apt-get -y install curl git
useradd hoogle
mkdir /home/hoogle
chown hoogle:hoogle /home/hoogle
cd /home/hoogle
curl -sSL https://get.haskellstack.org/ | sh
runuser -u hoogle bash <<HOOGLE_SETUP
git clone https://github.com/ndmitchell/hoogle.git
cd hoogle
git checkout 73fa6b5c156e0015e135a564e2821719611abe03
stack init --resolver=lts-14.7
stack build
stack install
export PATH=/home/hoogle/.local/bin:$PATH
mkdir daml
curl https://docs.daml.com/hoogle_db/base.txt --output daml/base.txt
hoogle generate --database=daml.hoo --local=daml
nohup hoogle server --database=daml.hoo --log=.log.txt --port=8080 >> out.txt &
HOOGLE_SETUP
cat > /home/hoogle/refresh-db.sh <<CRON
#!/usr/bin/env bash
set -euxo pipefail
log() {
  echo "[\$(date -Is)] \$1" >> /home/hoogle/cron_log.txt
}
log "Checking for new DAML version..."
cd /home/hoogle/hoogle
mkdir new-daml
if ! curl --fail -s https://docs.daml.com/hoogle_db/base.txt --output new-daml/base.txt; then
  curl -s https://docs.daml.com/hoogle_db.tar.gz --output db.tar.gz
  tar xzf db.tar.gz -C new-daml --strip-components=1
fi
if ! diff -rq daml new-daml; then
  log "New version detected. Creating database..."
  rm -rf daml
  mv new-daml daml
  rm daml.hoo
  /home/hoogle/.local/bin/hoogle generate --database=daml.hoo --local=daml
  log "Killing running instance..."
  killall hoogle
  log "Stating new server..."
  nohup /home/hoogle/.local/bin/hoogle server --database=daml.hoo --log=.log.txt --port=8080 >> out.txt &
  log "New server started."
else
  log "No change detected."
  rm -rf new-daml
fi
log "Done."
CRON
chmod +x /home/hoogle/refresh-db.sh
chown hoogle:hoogle /home/hoogle/refresh-db.sh
echo "*/5 * * * * /home/hoogle/refresh-db.sh" | crontab -u hoogle -
STARTUP

  network_interface {
    network       = "${google_compute_network.hoogle.name}"
    access_config = {}
  }

  service_account {
    email  = "log-writer@da-dev-gcp-daml-language.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
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

resource "google_compute_instance_group_manager" "hoogle" {
  provider           = "google-beta"
  name               = "hoogle"
  base_instance_name = "hoogle"
  zone               = "${local.zone}"
  target_size        = "3"

  version {
    name              = "hoogle"
    instance_template = "${google_compute_instance_template.hoogle.self_link}"
  }

  named_port {
    name = "https"
    port = "8080"
  }

  named_port {
    name = "http"
    port = "8081"
  }

  auto_healing_policies {
    health_check = "${google_compute_health_check.hoogle-https.self_link}"

    # Compiling hoogle takes some time
    initial_delay_sec = 2500
  }

  update_policy {
    type                  = "PROACTIVE"
    minimal_action        = "REPLACE"
    max_unavailable_fixed = 1
  }
}

resource "google_compute_global_address" "hoogle" {
  name       = "hoogle"
  ip_version = "IPV4"
}

resource "google_compute_health_check" "hoogle-http" {
  name               = "hoogle-http"
  check_interval_sec = 1
  timeout_sec        = 1

  tcp_health_check {
    port = 8081
  }
}

resource "google_compute_backend_service" "hoogle-http" {
  name          = "hoogle-http"
  health_checks = ["${google_compute_health_check.hoogle-http.self_link}"]
  port_name     = "http"

  backend {
    group = "${google_compute_instance_group_manager.hoogle.instance_group}"
  }
}

resource "google_compute_url_map" "hoogle-http" {
  name            = "hoogle-http"
  default_service = "${google_compute_backend_service.hoogle-http.self_link}"
}

resource "google_compute_target_http_proxy" "hoogle-http" {
  name    = "hoogle-http"
  url_map = "${google_compute_url_map.hoogle-http.self_link}"
}

resource "google_compute_global_forwarding_rule" "hoogle_http" {
  name       = "hoogle-http"
  target     = "${google_compute_target_http_proxy.hoogle-http.self_link}"
  ip_address = "${google_compute_global_address.hoogle.address}"
  port_range = "80"
}

resource "google_compute_health_check" "hoogle-https" {
  name               = "hoogle-https"
  check_interval_sec = 1
  timeout_sec        = 1

  tcp_health_check {
    port = 8080
  }
}

resource "google_compute_backend_service" "hoogle-https" {
  name          = "hoogle-https"
  health_checks = ["${google_compute_health_check.hoogle-https.self_link}"]
  port_name     = "https"

  backend {
    group = "${google_compute_instance_group_manager.hoogle.instance_group}"
  }
}

resource "google_compute_url_map" "hoogle-https" {
  name            = "hoogle-https"
  default_service = "${google_compute_backend_service.hoogle-https.self_link}"
}

resource "google_compute_target_https_proxy" "hoogle-https" {
  name    = "hoogle-https"
  url_map = "${google_compute_url_map.hoogle-https.self_link}"

  ssl_certificates = ["${local.ssl_certificate_hoogle}"]
}

resource "google_compute_global_forwarding_rule" "hoogle_https" {
  name       = "hoogle-https"
  target     = "${google_compute_target_https_proxy.hoogle-https.self_link}"
  ip_address = "${google_compute_global_address.hoogle.address}"
  port_range = "443"
}

output "hoogle_address" {
  value = "${google_compute_global_address.hoogle.address}"
}
