// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.{
  CommandRejected,
  TransactionAccepted,
}
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, DomainIndex, Update}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.daml.lf.data.Ref

import java.util.UUID

final case class PostPublishData(
    submissionDomainId: DomainId,
    publishSource: PublishSource,
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    actAs: Set[Ref.Party],
    offset: Offset,
    publicationTime: CantonTimestamp,
    submissionId: Option[Ref.SubmissionId],
    accepted: Boolean,
    traceContext: TraceContext,
)

object PostPublishData {
  def from(
      update: Traced[Update],
      offset: Offset,
      publicationTime: CantonTimestamp,
  ): Option[PostPublishData] = {
    def from(
        domainId: DomainId,
        domainIndex: Option[DomainIndex],
        accepted: Boolean,
    )(completionInfo: CompletionInfo): PostPublishData =
      PostPublishData(
        submissionDomainId = domainId,
        publishSource = completionInfo.messageUuid
          .map(PublishSource.Local(_): PublishSource)
          .getOrElse(
            PublishSource.Sequencer(
              requestSequencerCounter = domainIndex
                .flatMap(_.requestIndex)
                .flatMap(_.sequencerCounter)
                .getOrElse(
                  throw new IllegalStateException(
                    "If no messageUuid, then sequencer counter in request index should be present"
                  )
                ),
              sequencerTimestamp = domainIndex
                .flatMap(_.requestIndex)
                .map(_.timestamp)
                .getOrElse(
                  throw new IllegalStateException(
                    "If no messageUuid, then sequencer timestamp in request index should be present"
                  )
                ),
            )
          ),
        applicationId = completionInfo.applicationId,
        commandId = completionInfo.commandId,
        actAs = completionInfo.actAs.toSet,
        offset = offset,
        publicationTime = publicationTime,
        submissionId = completionInfo.submissionId,
        accepted = accepted,
        traceContext = update.traceContext,
      )

    update.value match {
      // please note: we pass into deduplication and inflight tracking only the transactions and not the transfers at acceptance
      case u: TransactionAccepted =>
        u.completionInfoO.map(from(u.domainId, u.domainIndex, accepted = true))
      // but: we pass into deduplication and inflight tracking both the transactions and the transfers upon rejection
      case u: CommandRejected =>
        Some(from(u.domainId, u.domainIndex, accepted = false)(u.completionInfo))
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
