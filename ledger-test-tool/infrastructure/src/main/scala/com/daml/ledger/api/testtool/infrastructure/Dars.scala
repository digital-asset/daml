// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.google.protobuf.ByteString

object Dars {

  def read(name: String): ByteString =
    ByteString.readFrom(getClass.getClassLoader.getResourceAsStream(name))

}
