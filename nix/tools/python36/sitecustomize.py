# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This sitecustomize module ensures that the SSL_CERT_FILE variable is
# set when Python runs.
#
# It replicates the logic from the nix.sh script that would normally
# be sourced by the python3.6 wrapper shell script. However, the
# wrapper shell script is not used inside virtual environments where
# all Python invocations go directly to the Python binary.

import os

# Set $SSL_CERT_FILE so that Nixpkgs applications like curl work.
paths = [
    '/etc/ssl/certs/ca-certificates.crt',  # NixOS, Ubuntu, Debian, Gentoo, ...
    '/etc/ssl/certs/ca-bundle.crt',        # Old NixOS
    '/etc/pki/tls/certs/ca-bundle.crt',    # Fedora, CentOS
    '~/.nix-profile/etc/ssl/certs/ca-bundle.crt',  # Nix profile
    '~/.nix-profile/etc/ca-bundle.crt',            # Old Nix profile
]

for path in paths:
    path = os.path.expanduser(path)
    if os.path.isfile(path):
        os.environ['SSL_CERT_FILE'] = path
        break
