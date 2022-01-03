// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.test.TestDar
import com.google.protobuf.ByteString

object Dars {

  // The list of all DAR packages that are bundled with this binary.
  // The TestDar object is generated by Bazel.
  val resources: List[String] = TestDar.paths

  def read(name: String): ByteString =
    ByteString.readFrom(getClass.getClassLoader.getResourceAsStream(name))

}
