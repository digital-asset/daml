// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

object Validation {
  def isReplayedBy(
      recorded: Transaction,
      replayed: Transaction,
  ): Either[ReplayMismatch, Unit] =
    if (recorded == replayed) {
      Right(())
    } else {
      Left(ReplayMismatch(recorded, replayed))
    }
}

final case class ReplayMismatch(
    recordedTransaction: Transaction,
    replayedTransaction: Transaction,
) extends Product
    with Serializable {
  def message: String =
    s"recreated and original transaction mismatch $recordedTransaction expected, but $replayedTransaction is recreated"
}
