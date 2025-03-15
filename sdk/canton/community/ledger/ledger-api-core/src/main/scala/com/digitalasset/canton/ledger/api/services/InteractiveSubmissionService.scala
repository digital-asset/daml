// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionResponse,
  PrepareSubmissionResponse,
  PreparedTransaction,
}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.Ref.{ApplicationId, SubmissionId}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey, SubmittedTransaction}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

object InteractiveSubmissionService {
  final case class PrepareRequest(commands: Commands, verboseHashing: Boolean)

  final case class TransactionData(
      submitterInfo: state.SubmitterInfo,
      transactionMeta: state.TransactionMeta,
      transaction: SubmittedTransaction,
      dependsOnLedgerTime: Boolean,
      globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
      inputContracts: Map[ContractId, FatContractInstance],
      synchronizerId: SynchronizerId,
  )

  final case class ExecuteRequest(
      applicationId: ApplicationId,
      submissionId: SubmissionId,
      deduplicationPeriod: DeduplicationPeriod,
      signatures: Map[PartyId, Seq[Signature]],
      preparedTransaction: PreparedTransaction,
      serializationVersion: HashingSchemeVersion,
      synchronizerId: SynchronizerId,
  )
}

trait InteractiveSubmissionService {
  def prepare(request: PrepareRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[PrepareSubmissionResponse]

  def execute(
      request: ExecuteRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[ExecuteSubmissionResponse]
}
