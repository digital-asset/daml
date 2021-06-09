# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//daml-lf/language:daml-lf.bzl",
    "versions",
)
load(
    "//daml-lf/language:daml-lf.bzl",
    "lf_version_configuration",
)

exceptions_suites = [
    "src/main/scala/com/daml/ledger/api/testtool/suites/ExceptionsIT.scala",
    "src/main/scala/com/daml/ledger/api/testtool/suites/ExceptionRaceConditionIT.scala",
]
exceptions_dummy_suites = [
    "src/main/scala/com/daml/ledger/api/testtool/dummy/ExceptionsIT.scala",
    "src/main/scala/com/daml/ledger/api/testtool/dummy/ExceptionRaceConditionIT.scala",
]

def suites_sources(version):
    suites = native.glob(
        ["src/main/scala/com/daml/ledger/api/testtool/suites/**/*.scala"],
        exclude = exceptions_suites,
    )

    if versions.gte(version, "1.14"):
        suites += exceptions_suites
    else:
        suites += exceptions_dummy_suites
    return suites
