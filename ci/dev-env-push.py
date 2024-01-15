#!/usr/bin/env python3
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# This script builds the dev-env nix closures and pushes them into the cache
import atexit
import os
import subprocess
import sys
import tempfile
import shutil

# Name of the Google Storage bucket where the nix cache is located
BUCKET_NAME = "daml-nix-cache"
TOP = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
NIX_CONF_DIR = os.path.join(TOP, "dev-env", "etc")


# Print logs
def log(*msg):
    # put everything on stderr to linearize the logs
    print(*msg, file=sys.stderr)


# Create a self-cleaning temporary working directory
def make_workdir():
    workdir = tempfile.mkdtemp()

    def unlink_workdir():
        shutil.rmtree(workdir)
        log("{} cleaned".format(workdir))
    atexit.register(unlink_workdir)

    return workdir


# Show the command that is about to be executed
def log_cmd(cmd):
    log("$", *cmd)


# Copies a secret from the environment into a file. Exits if the secret
# doesn't exist.
def get_secret(workdir, key):
    value = os.environ.pop(key, None)
    if value is None or value == '$({})'.format(key):
        log('could not find secret {}'.format(key))
        sys.exit(1)

    filepath = os.path.join(workdir, key)
    with open(filepath, "w") as file:
        file.write(value)

    return filepath


# Run nix-build and return the list of derivations
def nix_build(*args):
    cmd = ["nix-build", "--no-out-link", *args]
    env = {**os.environ, **dict(
        NIX_CONF_DIR=NIX_CONF_DIR,
        )}
    log_cmd(cmd)
    out_paths = subprocess.check_output(cmd, env=env).splitlines()
    return [path.decode('utf-8') for path in out_paths]


# Start the http server that acts as a nix store
def start_nix_gcs(google_creds):
    out = nix_build("./nix", "-A", "tools.nix-store-gcs-proxy")[0]
    env = {**os.environ, **dict(
            GOOGLE_APPLICATION_CREDENTIALS=google_creds,
            )}
    cmd = [
            os.path.join(out, "bin", "nix-store-gcs-proxy"),
            "--bucket-name", BUCKET_NAME,
            ]
    log_cmd(cmd)
    proc = subprocess.Popen(cmd, stdout=sys.stderr, env=env)

    def shutdown_nix_gcs():
        log("shutting down nix-store-gcs-proxy")
        proc.kill()
    atexit.register(shutdown_nix_gcs)

    return proc.pid


def main():
    workdir = make_workdir()

    nix_secret_key = get_secret(workdir, "NIX_SECRET_KEY_CONTENT")
    google_creds = get_secret(workdir, "GOOGLE_APPLICATION_CREDENTIALS_CONTENT")

    store_url = "http://localhost:3000?secret-key={secret_key}".format(
            secret_key=nix_secret_key,
            )

    start_nix_gcs(google_creds)

    # BUGFIX(zimbatm): clean the nix cache to force re-uploads
    nix_cache_dir = os.path.join(os.environ["HOME"], ".cache", "nix")
    if os.path.exists(nix_cache_dir):
        shutil.rmtree(nix_cache_dir)

    # copy to nix cache
    cmd = ["nix", "copy", "--to", store_url, "-f", "./nix", "tools", "ci-cached"]
    log_cmd(cmd)
    proc = subprocess.run(
            cmd,
            env={**os.environ, **dict(
                NIX_CONF_DIR=NIX_CONF_DIR,
                )}
            )

    sys.exit(proc.returncode)


main()
