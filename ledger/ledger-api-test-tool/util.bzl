# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//daml-lf/language:daml-lf.bzl",
    "versions",
)

exceptions_suite = "src/main/scala/com/daml/ledger/api/testtool/suites/ExceptionsIT.scala"
exceptions_dummy_suite = "src/main/scala/com/daml/ledger/api/testtool/dummy/ExceptionsIT.scala"

def suites_sources(version):
    suites = native.glob(
        ["src/main/scala/com/daml/ledger/api/testtool/suites/**/*.scala"],
        exclude = [exceptions_suite],
    )

    # TODO https://github.com/digital-asset/daml/issues/8020
    # Switch to a stable LF version.
    if versions.gte(version, "1.dev"):
        suites += [exceptions_suite]
    else:
        suites += [exceptions_dummy_suite]
    return suites
