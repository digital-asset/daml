# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

COMPAT_TAGS = ["manual", "compatibility"]

HEAD_VERSION = "0.0.0"

def daml_sdk(version):
    if version == HEAD_VERSION:
        return "//compatibility/head:dpm"
    return "@compat_sdks//:daml-{}".format(version)
