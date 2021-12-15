// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox.bridge

import ledger.offset.Offset
import ledger.participant.state.v2.Update
import ledger.sandbox.bridge.LedgerBridge.{successMapper, toOffset}
import ledger.sandbox.domain.Submission
import lf.data.Ref

import akka.NotUsed
import akka.stream.scaladsl.Flow

private[bridge] class PassThroughLedgerBridge(participantId: Ref.ParticipantId)
    extends LedgerBridge {
  override def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission].zipWithIndex
      .map { case (submission, index) =>
        val nextOffset = toOffset(index)
        val update = successMapper(submission, index, participantId)
        nextOffset -> update
      }
}
