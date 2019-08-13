// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.util.UUID

import com.digitalasset.daml.lf.data.Ref

object PartyIdGenerator {
  def generateRandomId(): Ref.Party =
    Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString.take(8)}")
}
