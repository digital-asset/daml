// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.daml.lf.testing

import com.digitalasset.canton.LfPartyId

object SampleParties {
  val AlicesBank = LfPartyId.assertFromString("AlicesBank::default")
  val BobsBank = LfPartyId.assertFromString("BobsBank::default")
  val Alice = LfPartyId.assertFromString("Alice::default")
  val Bob = LfPartyId.assertFromString("Bob::default")
  val Charlie = LfPartyId.assertFromString("Charlie::default")
}
