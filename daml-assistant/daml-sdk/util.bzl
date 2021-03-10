# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def deps(edition):
    return [
        "//daml-script/runner:script-runner-lib-{}".format(edition),
        "//extractor",
        "//language-support/codegen-main:codegen-main-lib",
        "//ledger-service/http-json",
        "//ledger/sandbox:sandbox-{}".format(edition),
        "//ledger/sandbox-classic:sandbox-classic-{}".format(edition),
        "//navigator/backend:navigator-library",
        "//triggers/runner:trigger-runner-lib",
        "//triggers/service:trigger-service",
        "//triggers/service/auth:oauth2-middleware",
        "//navigator/backend:backend-resources",
        "//navigator/backend:frontend-resources",
    ]
