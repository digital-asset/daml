// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.google.protobuf.ByteString

object Dars {

  // The list of all DAR packages that are bundled with this binary.
  // The TestDars object is generated by Bazel.
  val resources: List[String] =
    com.daml.ledger.test_common.TestDars.fileNames.values.toList

  def read(name: String): ByteString =
    ByteString.readFrom(getClass.getClassLoader.getResourceAsStream(name))

}
