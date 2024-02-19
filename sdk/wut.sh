#!/bin/bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


ls -al bazel-out/k8-opt/bin/canton/Com/Daml/Ledger/Api/V2

cd /home/jarek/.cache/bazel/_bazel_jarek/4ed3639164c8b5b1b6a323744137623b/execroot/com_github_digital_asset_daml && \
  exec env - \
    GIT_SSL_CAINFO=/nix/store/w22mgz86w3nqjc91xmcpngssnb0pj0k8-nss-cacert-3.86/etc/ssl/certs/ca-bundle.crt \
    LOCALE_ARCHIVE=/nix/store/qgiwzr3rdw73r72g4v6ifakh5w56q7az-glibc-locales-2.35-224/lib/locale/locale-archive \
    PATH=/nix/store/4xw8n979xpivdc46a9ndcvyhwgif00hz-bash-5.1-p16/bin:/nix/store/h8gvq6r4hgpa71h44dmg9qfx03mj81sv-coreutils-9.1/bin:/nix/store/qss2l7hpp4km2msil0g1xfg82774h81v-file-5.43/bin:/nix/store/zml88vnkpm8if114qkbbqd1q7n3ypqqy-findutils-4.9.0/bin:/nix/store/l1fp0hyca54xbb85vfhppd16bskzx8dg-gawk-5.1.1/bin:/nix/store/bcvccw6y9bfil6xrl5j7psza7hnd16ry-gnugrep-3.7/bin:/nix/store/5dv5cq1lwvsijr9p16p2kp79g1dbajk3-gnused-4.8/bin:/nix/store/89zbjdkb48ma61k76l2mzn3s0ra0wn2c-gnutar-1.34/bin:/nix/store/qs8qb1swpivkfq7i9yd52n0mw6z4ij81-gzip-1.12/bin:/nix/store/al6g1zbk8li6p8mcyp0h60d08jaahf8c-python3-3.10.9/bin:/nix/store/96c7bdwfm7yj65lr2867bmm2fi01x7jx-unzip-6.0/bin:/nix/store/kzw8jpmwjwldkzkf2sha1brg8l19rqh5-which-2.21/bin:/nix/store/l54ibwj1mg1gjdppqxn9ap313jlz5r5f-zip-3.0/bin \
  /nix/store/4xw8n979xpivdc46a9ndcvyhwgif00hz-bash-5.1-p16/bin/bash -c 'source external/bazel_tools/tools/genrule/genrule-setup.sh;
        for src in canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/checkpoint.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/command_completion_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/command_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/command_submission_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/commands.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/completion.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/event.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/event_query_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/experimental_features.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/package_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/participant_offset.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/reassignment.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/reassignment_command.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/state_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/trace_context.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/transaction.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/transaction_filter.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/update_service.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/value.proto canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/version_service.proto; do
            bazel-out/host/bin/external/proto3-suite/compile-proto-file                 --includeDir external/com_google_protobuf/src                 --includeDir external/go_googleapis                 --includeDir canton/community/ledger-api/src/main/protobuf                 --proto ${src#*community/ledger-api/src/main/protobuf/}                 --out bazel-out/k8-opt/bin/canton/Com/Daml/Ledger/Api/V2
        done
    '


ls -al bazel-out/k8-opt/bin/canton/Com/Daml/Ledger/Api/V2
