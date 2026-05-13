// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.google.protobuf.ByteString

object Dars {

  // The list of all DAR packages that are bundled with this binary.
  def resources(lfVersion: String): List[String] = TestDar.paths(lfVersion)

  def read(name: String): ByteString =
    ByteString.readFrom(getClass.getClassLoader.getResourceAsStream(name))

}
