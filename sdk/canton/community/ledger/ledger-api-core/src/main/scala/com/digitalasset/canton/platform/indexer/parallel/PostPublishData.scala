// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp}
import com.digitalasset.canton.ledger.participant.state.Update.{
  SequencedCommandRejected,
  SequencedTransactionAccepted,
  UnSequencedCommandRejected,
}
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref

import java.util.UUID

final case class PostPublishData(
    submissionDomainId: DomainId,
    publishSource: PublishSource,
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    actAs: Set[Ref.Party],
    offset: AbsoluteOffset,
    publicationTime: CantonTimestamp,
    submissionId: Option[Ref.SubmissionId],
    accepted: Boolean,
    traceContext: TraceContext,
)

object PostPublishData {
  def from(
      update: Update,
      offset: AbsoluteOffset,
      publicationTime: CantonTimestamp,
  ): Option[PostPublishData] = {
    def from(
        domainId: DomainId,
        publishSource: PublishSource,
        completionInfo: CompletionInfo,
        accepted: Boolean,
    ): PostPublishData =
      PostPublishData(
        submissionDomainId = domainId,
        publishSource = publishSource,
        applicationId = completionInfo.applicationId,
        commandId = completionInfo.commandId,
        actAs = completionInfo.actAs.toSet,
        offset = offset,
        publicationTime = publicationTime,
        submissionId = completionInfo.submissionId,
        accepted = accepted,
        traceContext = update.traceContext,
      )

    update match {
      // please note: we pass into deduplication and inflight tracking only the transactions and not the reassignments at acceptance
      case u: SequencedTransactionAccepted =>
        u.completionInfoO.map(completionInfo =>
          from(
            domainId = u.domainId,
            publishSource = PublishSource.Sequencer(
              requestSequencerCounter = u.sequencerCounter,
              sequencerTimestamp = u.recordTime,
            ),
            completionInfo = completionInfo,
            accepted = true,
          )
        )

      // but: we pass into deduplication and inflight tracking both the transactions and the reassignments upon rejection
      case u: SequencedCommandRejected =>
        Some(
          from(
            domainId = u.domainId,
            publishSource = PublishSource.Sequencer(
              requestSequencerCounter = u.sequencerCounter,
              sequencerTimestamp = u.recordTime,
            ),
            completionInfo = u.completionInfo,
            accepted = false,
          )
        )

      case u: UnSequencedCommandRejected =>
        Some(
          from(
            domainId = u.domainId,
            publishSource = PublishSource.Local(
              messageUuid = u.messageUuid
            ),
            completionInfo = u.completionInfo,
            accepted = false,
          )
        )

      case _ => None
    }
  }
}

sealed trait PublishSource extends Product with Serializable
object PublishSource {
  final case class Local(messageUuid: UUID) extends PublishSource
  final case class Sequencer(
      requestSequencerCounter: SequencerCounter,
      sequencerTimestamp: CantonTimestamp,
  ) extends PublishSource
}
