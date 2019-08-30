// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.ActiveContract

case class LedgerSnapshot(offset: Long, acs: Source[(AbsoluteContractId, ActiveContract), NotUsed])
