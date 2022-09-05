// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox.bridge

import ledger.offset.Offset
import ledger.participant.state.v2.Update
import ledger.sandbox.bridge.LedgerBridge.{
  configChangedSuccess,
  packageUploadSuccess,
  partyAllocationSuccess,
  toOffset,
  transactionAccepted,
}
import ledger.sandbox.domain.Submission
import lf.data.{Ref, Time}
import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.api.util.TimeProvider

private[bridge] class PassThroughLedgerBridge(
    participantId: Ref.ParticipantId,
    timeProvider: TimeProvider,
) extends LedgerBridge {
  override def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission].zipWithIndex
      .map { case (submission, index) =>
        val nextOffset = toOffset(index)
        val update =
          successMapper(submission, index, participantId, timeProvider.getCurrentTimestamp)
        nextOffset -> update
      }

  private def successMapper(
      submission: Submission,
      index: Long,
      participantId: Ref.ParticipantId,
      currentTimestamp: Time.Timestamp,
  ): Update =
    submission match {
      case s: Submission.AllocateParty => partyAllocationSuccess(s, participantId, currentTimestamp)
      case s: Submission.Config => configChangedSuccess(s, participantId, currentTimestamp)
      case s: Submission.UploadPackages => packageUploadSuccess(s, currentTimestamp)
      case s: Submission.Transaction => transactionAccepted(s, index, currentTimestamp)
    }
}
