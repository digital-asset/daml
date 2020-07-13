# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

setvar() {
    echo "Setting '$1' to '$2'"
    echo "##vso[task.setvariable variable=$1;isOutput=true]$2"
}
