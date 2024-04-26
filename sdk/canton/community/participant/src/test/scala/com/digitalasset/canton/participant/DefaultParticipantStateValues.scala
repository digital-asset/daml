// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.syntax.option.*
import com.daml.lf.data.Ref
import com.digitalasset.canton.DefaultDamlValues.*
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{ApplicationId, CommandId, DefaultDamlValues, WorkflowId}

/** Default values for participant state objects for unit testing */
object DefaultParticipantStateValues {
  def submitterInfo(
      actAs: List[Ref.Party],
      applicationId: ApplicationId = DefaultDamlValues.applicationId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
      deduplicationPeriod: DeduplicationPeriod = deduplicationDuration,
      submissionId: Option[Ref.SubmissionId] = DefaultDamlValues.submissionId().some,
  ): SubmitterInfo =
    SubmitterInfo(
      actAs,
      List.empty, // readAs parties in submitter info are ignored by canton
      applicationId.unwrap,
      commandId.unwrap,
      deduplicationPeriod,
      submissionId,
    )

  def completionInfo(
      actAs: List[Ref.Party],
      applicationId: ApplicationId = DefaultDamlValues.applicationId(),
      commandId: CommandId = DefaultDamlValues.commandId(),
      optDeduplicationPeriod: Option[DeduplicationPeriod] = Some(deduplicationDuration),
      submissionId: Option[Ref.SubmissionId] = DefaultDamlValues.submissionId().some,
      statistics: Option[LedgerTransactionNodeStatistics] = None,
  ): CompletionInfo =
    CompletionInfo(
      actAs,
      applicationId.unwrap,
      commandId.unwrap,
      optDeduplicationPeriod,
      submissionId,
      statistics,
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
