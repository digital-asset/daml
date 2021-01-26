# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def pkg_empty_zip(name, out):
    native.genrule(
        name = name,
        srcs = [],
        outs = [out],
        # minimal empty zip file in Base64 encoding
        cmd = "echo UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA== | base64 -d > $@",
    )
