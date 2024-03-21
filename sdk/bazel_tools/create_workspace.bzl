# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _create_workspace_impl(repository_ctx):
    for (filename, content) in repository_ctx.attr.files.items():
        repository_ctx.file(filename, content, False)

create_workspace = repository_rule(
    _create_workspace_impl,
    attrs = {
        "files": attr.string_dict(),
    },
)
