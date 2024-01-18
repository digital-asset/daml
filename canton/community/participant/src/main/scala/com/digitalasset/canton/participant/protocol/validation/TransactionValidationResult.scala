// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.{SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, WorkflowId}

final case class TransactionValidationResult(
    transactionId: TransactionId,
    confirmationPolicy: ConfirmationPolicy,
    submitterMetadataO: Option[SubmitterMetadata],
    workflowIdO: Option[WorkflowId],
    contractConsistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
    authenticationResult: Map[ViewPosition, String],
    authorizationResult: Map[ViewPosition, String],
    modelConformanceResultE: Either[
      ModelConformanceChecker.ErrorWithSubTransaction,
      ModelConformanceChecker.Result,
    ],
    internalConsistencyResultE: Either[ErrorWithInternalConsistencyCheck, Unit],
    consumedInputsOfHostedParties: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    createdContracts: Map[LfContractId, SerializableContract],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    successfulActivenessCheck: Boolean,
    viewValidationResults: Map[ViewPosition, ViewValidationResult],
    timeValidationResultE: Either[TimeCheckFailure, Unit],
    hostedWitnesses: Set[LfPartyId],
    replayCheckResult: Option[String],
) {

  def commitSet(
      requestId: RequestId
  )(protocolVersion: ProtocolVersion)(implicit loggingContext: ErrorLoggingContext): CommitSet =
    CommitSet.createForTransaction(
      successfulActivenessCheck,
      requestId,
      consumedInputsOfHostedParties,
      transient,
      createdContracts,
    )(protocolVersion)
}
