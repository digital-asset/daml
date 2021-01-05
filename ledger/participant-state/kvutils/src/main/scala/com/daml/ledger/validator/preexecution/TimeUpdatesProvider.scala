// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.lf.data.Time.Timestamp

object TimeUpdatesProvider {
  private[this] val NowFakeTimeUpdatesProvider: TimeUpdatesProvider = () => Some(Timestamp.now())

  val ReasonableDefault: TimeUpdatesProvider = NowFakeTimeUpdatesProvider
}
