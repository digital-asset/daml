// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.lf.data.Time.Timestamp

/**
  * Produces the record time on the participant for updates originating from pre-executed submissions.
  */
trait TimeUpdatesProvider {
  def retrieveTimeUpdate(): Option[Timestamp]
}

object TimeUpdatesProvider {
  private[this] val NowFakeTimeUpdatesProvider: TimeUpdatesProvider = () => Some(Timestamp.now())

  // TODO replace with actual time update log entry read
  val ReasonableDefault: TimeUpdatesProvider = NowFakeTimeUpdatesProvider
}
