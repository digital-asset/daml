#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Agent startup script
set -euo pipefail

## Hardening

# Commit harakiri on failure
trap "shutdown -h now" EXIT

# replace the default nameserver to not use the metadata server
echo "nameserver 8.8.8.8" > /etc/resolv.conf

# delete self
rm -vf "$0"

## Install system dependencies
apt-get update -q
apt-get install -qy \
  curl sudo \
  bzip2 rsync \
  jq liblttng-ust0 libcurl3 libkrb5-3 libicu55 zlib1g \
  git \
  netcat \
  apt-transport-https \
  software-properties-common \
  gcc

# Install dependencies for Chrome (to run Puppeteer tests on the gsg)
# list taken from: https://github.com/puppeteer/puppeteer/blob/a3d1536a6b6e282a43521bea28aef027a7133df8/docs/troubleshooting.md#chrome-headless-doesnt-launch-on-unix
# see https://github.com/digital-asset/daml/pull/5540 for context
apt-get install -qy \
    gconf-service \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgcc1 \
    libgconf-2-4 \
    libgdk-pixbuf2.0-0 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    ca-certificates \
    fonts-liberation \
    libappindicator1 \
    libnss3 \
    lsb-release \
    xdg-utils \
    wget

curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash

#install docker
DOCKER_VERSION="5:18.09.5~3-0~ubuntu-$(lsb_release -cs)"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -qy docker-ce=$DOCKER_VERSION docker-ce-cli=$DOCKER_VERSION containerd.io

#Start docker daemon
systemctl enable docker

## Install the VSTS agent
groupadd --gid 3000 vsts
useradd \
  --create-home \
  --gid 3000 \
  --shell /bin/bash \
  --uid 3000 \
  vsts
#add docker group to user
usermod -aG docker vsts

# The Bazel cache fills up the hard drive in less than a day, and is a real
# pain to clean up, as it consists of a very large amount of read-only files in
# a fairly large tree of read-only directories. Therefore, it looks like the
# best way to manage it is to mount it on a partition and reset the partition.

cat <<REMOUNT_CACHE > /root/remount_cache.c
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

int exists(const char* path) {
	struct stat s;
	return stat(path, &s) == 0 && S_ISDIR(s.st_mode);
}

int execute_f(char** args) {
	int status;
	char** child_env = { NULL };
	pid_t pid = fork();
	if (0 == pid) {
		// child process
		if (-1 == execve(args[0], args, child_env)) {
			printf("Failed to start child process calling %s.", args[0]);
			perror(0);
			exit(1);
		}
		// unreachable: if execve succeeded, we're off to another executable
	}
	// parent
	if (-1 == pid) {
		printf("Failed to fork.");
		perror(0);
		exit(1);
	}
	if (-1 == waitpid(pid, &status, 0)) {
		printf("Failed to wait for child process '%s'.", args[0]);
		exit(1);
	}
	if (1 != WIFEXITED(status)) {
		printf("Child not terminated: '%s', aborting.", args[0]);
		exit(1);
	}
	if (0 != WEXITSTATUS(status)) {
		printf("Child '%s' exited with status %d", args[0], WEXITSTATUS(status));
		exit(WEXITSTATUS(status));
	}
	return 0;
}

#define execute(...) execute_f((char*[]) { __VA_ARGS__, NULL })

int main() {
	char* mount_point = "/home/vsts/.cache/bazel";
	char* lost = "/home/vsts/.cache/bazel/lost+found";
	char* file = "/root/cache_file";

	setuid(0);

	if (exists(lost)) {
		printf("Removing existing cache...");
        // This will fail if there are dangling processes, say Bazel servers,
        // with open fds on the cache.
		execute("/bin/umount", mount_point);
	}

	execute("/bin/rm", "-f", file);
    // truncate creates a sparse file. In my testing, for this use-case, there
    // does not seem to be a significant overhead.
	execute("/usr/bin/truncate", "-s", "200g", file);
	execute("/sbin/mkfs.ext3", file);
	execute("/bin/mount", file, mount_point);
	execute("/bin/chown", "-R", "vagrant:vagrant", mount_point);
	return 0;
}
REMOUNT_CACHE

mkdir -p /home/vsts/.cache/bazel
chown -R vsts:vsts /home/vsts/.cache
( cd /root && gcc remount_cache.c -o /usr/local/bin/remount_cache )
chmod ug+s /usr/local/bin/remount_cache
/usr/local/bin/remount_cache

# Done with the cache cleanup stuff.


su --login vsts <<'AGENT_SETUP'
set -euo pipefail

VSTS_ACCOUNT=${vsts_account}
VSTS_POOL=${vsts_pool}
VSTS_TOKEN=${vsts_token}

mkdir -p ~/agent
cd ~/agent
echo 'assignment=default' > .capabilities

echo Determining matching VSTS agent...
VSTS_AGENT_RESPONSE=$(curl -sSfL \
  -u "user:$VSTS_TOKEN" \
  -H 'Accept:application/json;api-version=3.0-preview' \
  "https://$VSTS_ACCOUNT.visualstudio.com/_apis/distributedtask/packages/agent?platform=linux-x64")

VSTS_AGENT_URL=$(echo "$VSTS_AGENT_RESPONSE" \
  | jq -r '.value | map([.version.major,.version.minor,.version.patch,.downloadUrl]) | sort | .[length-1] | .[3]')

if [ -z "$VSTS_AGENT_URL" -o "$VSTS_AGENT_URL" == "null" ]; then
  echo 1>&2 error: could not determine a matching VSTS agent - check that account \'$VSTS_ACCOUNT\' is correct and the token is valid for that account
  exit 1
fi

echo Downloading and installing VSTS agent...
curl -sSfL "$VSTS_AGENT_URL" | tar -xz --no-same-owner

set +u
source ./env.sh
set -u

./config.sh \
  --acceptTeeEula \
  --agent "$(hostname)" \
  --auth PAT \
  --pool "$VSTS_POOL" \
  --replace \
  --token "$VSTS_TOKEN" \
  --unattended \
  --url "https://$VSTS_ACCOUNT.visualstudio.com"
AGENT_SETUP

## Hardening

chown --recursive root:root /home/vsts/agent/{*.sh,bin,externals}

## Install Nix

# This needs to run inside of a user with sudo access
echo "vsts ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/nix_installation
su --command "sh <(curl -sSfL https://nixos.org/nix/install) --daemon" --login vsts
rm /etc/sudoers.d/nix_installation

# Note: the "hydra.da-int.net" string is now part of the name of the key for
# legacy reasons; it bears no relation to the DNS hostname of the current
# cache.
cat <<NIX_CONF > /etc/nix/nix.conf
binary-cache-public-keys = hydra.da-int.net-2:91tXuJGf/ExbAz7IWsMsxQ5FsO6lG/EGM5QVt+xhZu0= hydra.da-int.net-1:6Oy2+KYvI7xkAOg0gJisD7Nz/6m8CmyKMbWfSKUe03g= cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= hydra.nixos.org-1:CNHJZBh9K4tP3EKF6FkkgeVYsS3ohTl+oS0Qa8bezVs=
binary-caches = https://nix-cache.da-ext.net https://cache.nixos.org
build-users-group = nixbld
cores = 1
max-jobs = 0
sandbox = relaxed
NIX_CONF

systemctl restart nix-daemon

# Warm up local caches by building dev-env and current daml main
# This is allowed to fail, as we still want to have CI machines
# around, even when their caches are only warmed up halfway
su --login vsts <<'CACHE_WARMUP'
# user-wide bazel disk cache override
echo "build:linux --disk_cache=~/.bazel-cache" > ~/.bazelrc

# clone and build
(
  git clone https://github.com/digital-asset/daml
  cd daml
  ./ci/dev-env-install.sh
  ./build.sh "_$(uname)"
) || true
CACHE_WARMUP

# Remove /home/vsts/daml folder that might be present from cache warmup
rm -R /home/vsts/daml || true

## Finish

# run the fake local webserver, taken from the docker image
web-server() {
  while true; do
    printf 'HTTP/1.1 302 Found\r\nLocation: https://%s.visualstudio.com/_admin/_AgentPool\r\n\r\n' "${vsts_account}" | nc -l -p 80 -q 0 > /dev/null
  done
}
web-server &

# Start the VSTS agent
su --login --command "cd /home/vsts/agent && exec ./run.sh" - vsts
