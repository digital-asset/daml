// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.ledger.configuration.LedgerTimeModel

private sealed trait TimeModelError {
  def message: String
}

private object TimeModelError {
  object NoLedgerConfiguration extends TimeModelError {
    val message: String =
      "No ledger configuration available, cannot validate ledger time"
  }

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange)
      extends TimeModelError {
    lazy val message: String =
      outOfRange.message
  }
}
