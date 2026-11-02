# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@go_googleapis//:repository_rules.bzl", "switched_rules_by_language")

def _impl(module_ctx):
    switched_rules_by_language(
        name = "com_google_googleapis_imports",
        grpc = True,
        java = True,
    )

import_googleapis_extension = module_extension(implementation = _impl)
