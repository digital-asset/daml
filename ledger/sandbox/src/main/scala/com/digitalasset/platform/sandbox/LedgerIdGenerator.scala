// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.UUID

import com.digitalasset.daml.lf.data.Ref

object LedgerIdGenerator {
  def generateRandomId(): Ref.LedgerName =
    Ref.LedgerName.assertFromString(s"sandbox-${UUID.randomUUID().toString}")
}
