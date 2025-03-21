// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.syntax.option.*
import com.digitalasset.canton.DefaultDamlValues.*
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{CommandId, DefaultDamlValues, LfPartyId, UserId, WorkflowId}
import com.digitalasset.daml.lf.data.Ref

/** Default values for participant state objects for unit testing */
object DefaultParticipantStateValues {

  def changeId(
      actAs: Set[LfPartyId],
      userId: UserId = DefaultDamlValues.userId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
  ): ChangeId =
    ChangeId(userId.unwrap, commandId.unwrap, actAs)

  def submitterInfo(
      actAs: List[Ref.Party],
      userId: UserId = DefaultDamlValues.userId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
      deduplicationPeriod: DeduplicationPeriod = deduplicationDuration,
      submissionId: Option[Ref.SubmissionId] = DefaultDamlValues.submissionId().some,
  ): SubmitterInfo =
    SubmitterInfo(
      actAs,
      List.empty, // readAs parties in submitter info are ignored by canton
      userId.unwrap,
      commandId.unwrap,
      deduplicationPeriod,
      submissionId,
      externallySignedSubmission = None,
    )

  def completionInfo(
      actAs: List[Ref.Party],
      userId: UserId = DefaultDamlValues.userId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
      optDeduplicationPeriod: Option[DeduplicationPeriod] = Some(deduplicationDuration),
      submissionId: Option[Ref.SubmissionId] = DefaultDamlValues.submissionId().some,
  ): CompletionInfo =
    CompletionInfo(
      actAs,
      userId.unwrap,
      commandId.unwrap,
      optDeduplicationPeriod,
      submissionId,
    )

  def transactionMeta(
      ledgerEffectiveTime: CantonTimestamp = CantonTimestamp.Epoch,
      workflowId: Option[WorkflowId] = None,
      submissionTime: CantonTimestamp = CantonTimestamp.Epoch,
      submissionSeed: LfHash = lfhash(),
  ): TransactionMeta =
    TransactionMeta(
      ledgerEffectiveTime.toLf,
      workflowId.map(_.unwrap),
      submissionTime.toLf,
      submissionSeed,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )
}
