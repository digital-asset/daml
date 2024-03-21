# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def deps(edition):
    return [
        "//daml-script/runner:script-runner-lib",
        "//language-support/codegen-main:codegen-main-lib",
        "//ledger-service/http-json:http-json-{}".format(edition),
        "//navigator/backend:navigator-library",
        "//daml-script/export",
        "//triggers/runner:trigger-runner-lib",
        "//triggers/service:trigger-service-binary-{}".format(edition),
        "//triggers/service/auth:oauth2-middleware",
        "//navigator/backend:backend-resources",
        "//navigator/backend:frontend-resources",
    ]
