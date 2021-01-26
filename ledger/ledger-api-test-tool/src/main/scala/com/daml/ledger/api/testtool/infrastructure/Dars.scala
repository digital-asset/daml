// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.google.protobuf.ByteString

import scala.collection.immutable

object Dars {

  // This must be kept aligned manually with artifacts declared in /ledger/test-common/BUILD.bazel.
  val resources: immutable.Seq[String] = immutable.Seq(
    "/ledger/test-common/model-tests.dar",
    "/ledger/test-common/performance-tests.dar",
    "/ledger/test-common/semantic-tests.dar",
  )

  def read(name: String): ByteString =
    ByteString.readFrom(getClass.getResourceAsStream(name))

}
