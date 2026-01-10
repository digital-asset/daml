// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.google.protobuf.ByteString

object Dars {

  // The list of all DAR packages that are bundled with this binary.
  def resources(lfVersion: String): List[String] = TestDar.paths(lfVersion)

  def read(name: String): ByteString =
    ByteString.readFrom(getClass.getClassLoader.getResourceAsStream(name))

}
