# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def deps(edition):
    return [
        "//daml-script/runner:script-runner-lib",
        "//language-support/codegen-main:codegen-main-lib",
    ]
