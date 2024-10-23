// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.ledger.api.LfValueParty

sealed abstract class Choice[R](val consuming: Boolean) {

  def controllers: Seq[LfValueParty] = {
    Seq.empty
  }

  def observers: Seq[LfValueParty] = {
    Seq.empty
  }

  def authorizers: Seq[LfValueParty] = {
    Seq.empty
  }

  def exercise(): R
}

abstract class ConsumingChoice[R] extends Choice[R](consuming = true)

abstract class NonConsumingChoice[R] extends Choice[R](consuming = false)
