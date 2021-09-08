// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

private final class Validation[Nid]() {

  /** Whether `replayed` is the result of reinterpreting this transaction.
    *
    * @param recorded : the transaction to be validated.
    * @param replayed : the transaction resulting from the reinterpretation of
    *   the root nodes of [[recorded]].
    * @note This function is symmetric
    *
    * 'isReplayedBy' is simply structural equality.
    *
    * The RIGHT `replayed` arg requires *no* normalization since TXs coming from the
    * engine are now normalized.
    */

  private def isReplayedBy(
      recorded: VersionedTransaction[Nid],
      replayed: VersionedTransaction[Nid],
  ): Either[ReplayMismatch[Nid], Unit] = {
    if (recorded == replayed) {
      Right(())
    } else {
      Left(ReplayMismatch(recorded, replayed))
    }
  }
}

object Validation {
  def isReplayedBy[Nid](
      recorded: VersionedTransaction[Nid],
      replayed: VersionedTransaction[Nid],
  ): Either[ReplayMismatch[Nid], Unit] =
    new Validation().isReplayedBy(recorded, replayed)
}

final case class ReplayMismatch[Nid](
    recordedTransaction: VersionedTransaction[Nid],
    replayedTransaction: VersionedTransaction[Nid],
) extends Product
    with Serializable {
  def message: String =
    s"recreated and original transaction mismatch $recordedTransaction expected, but $replayedTransaction is recreated"
}
