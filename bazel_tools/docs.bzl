# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build rule for a DA Documentation Package.
def da_doc_package(**args):
    src_name = args["name"] + "-srcs"
    native.filegroup(
        name = src_name,
        srcs = native.glob(["source/**"]),
        )
    prepare = args.get("prepare", "")
    extra_srcs = args.get("extra_srcs", [])
    native.genrule(
       name = args["name"],
       outs = ["sources.tar.gz"],
       srcs = [src_name, "@tar_dev_env//:tar", "@gzip_dev_env//:gzip"] + extra_srcs,
       cmd = """
            export PATH=$$(dirname $(location {gzip})):$$PATH
            # Bazel makes it fairly hard to get the directory in a robust way so
            # we take the first file and strip everything after source.
            SRCS=($(locations {srcs}))
            BASEDIR=$${{SRCS[0]%%source*}}
            {prepare}
            $(location {tar}) czhf $(location sources.tar.gz) -C $$BASEDIR source
        """.format(
              srcs = ":" + src_name,
              prepare = prepare,
              tar = "@tar_dev_env//:tar",
              gzip = "@gzip_dev_env//:gzip",
            )
        )
