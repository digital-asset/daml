// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.ledger

import com.google.protobuf.ByteString

package object api {
  def logInfo(msg: String): Unit = {
    api.internal.host.logInfo(msg)
  }
}
