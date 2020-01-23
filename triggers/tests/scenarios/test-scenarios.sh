# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

# Source files are not found by rlocation.
SDK_VERSION=$1
DAMLC=$2
DAML_TRIGGERS_DAR=$3
DAML_SOURCE=$4

TMP_DIR=$(mktemp -d)
mkdir -p $TMP_DIR/daml
cat <<EOF > $TMP_DIR/daml.yaml
sdk-version: $(cat $SDK_VERSION)
name: trigger-scenarios
source: daml
version: 0.0.1
dependencies:
  - daml-stdlib
  - daml-prim
  - daml-trigger.dar
EOF
cp -L $DAML_TRIGGERS_DAR $TMP_DIR/
cp -L $DAML_SOURCE $TMP_DIR/daml/

$DAMLC test --project-root=$TMP_DIR
