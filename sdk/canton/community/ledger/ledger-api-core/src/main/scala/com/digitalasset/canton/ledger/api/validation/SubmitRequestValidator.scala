// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.daml.ledger.api.v2.command_submission_service.{SubmitReassignmentRequest, SubmitRequest}
import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  PartySignatures,
  PrepareSubmissionRequest,
  SinglePartySignatures,
}
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.messages.command.submission
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.{PartyId as TopologyPartyId, SynchronizerId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.canton.version.HashingSchemeVersion.V2
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag.*

import java.time.{Duration, Instant}

class SubmitRequestValidator(
    commandsValidator: CommandsValidator
) {
  import FieldValidator.*
  def validate(
      req: SubmitRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
    } yield submission.SubmitRequest(validatedCommands)

  def validatePrepare(
      req: PrepareSubmissionRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, InteractiveSubmissionService.PrepareRequest] =
    for {
      validatedCommands <- commandsValidator.validatePrepareRequest(
        req,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
      maxRecordTime <- req.maxRecordTime.traverse(commandsValidator.validateLfTime)
    } yield InteractiveSubmissionService.PrepareRequest(
      validatedCommands,
      req.verboseHashing,
      maxRecordTime,
    )

  private def validatePartySignatures(
      proto: PartySignatures
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Map[TopologyPartyId, Seq[Signature]]] =
    proto.signatures
      .traverse { case SinglePartySignatures(partyP, signaturesP) =>
        for {
          partyId <- requireTopologyPartyIdField(partyP, "SinglePartySignatures.party")
          signatures <- signaturesP.traverse(s =>
            CryptoValidator.validateSignature(s, "SinglePartySignatures.signature")
          )
        } yield partyId -> signatures
      }
      .map(_.foldLeft(Map.empty[TopologyPartyId, Seq[Signature]]) { case (m, (p, s)) =>
        m.updatedWith(p) {
          case None => Some(s)
          // This covers the test case where a client submits multiple SinglePartySignatures
          // objects for a single party (the more usual use case would be to submit all signatures in one go)
          case Some(existing) => Some((s.toSet ++ existing.toSet).toSeq)
        }
      })

  def validateExecute(
      req: iss.ExecuteSubmissionRequest,
      currentLedgerTime: Instant,
      submissionIdGenerator: SubmissionIdGenerator,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ExecuteRequest] = {
    val iss.ExecuteSubmissionRequest(
      preparedTransactionP,
      partySignaturesOP,
      deduplicationPeriodP,
      submissionIdP,
      userIdP,
      hashingSchemeVersionP,
      minLedgerTimeP,
    ) = req
    for {
      submissionId <- validateSubmissionId(submissionIdP)
        .map(_.map(_.unwrap))
        .map(
          _.getOrElse(submissionIdGenerator.generate())
        )
      userId <- requireUserId(userIdP, "user_id")
      deduplicationPeriod <- commandsValidator.validateExecuteDeduplicationPeriod(
        deduplicationPeriodP,
        maxDeduplicationDuration,
      )
      preparedTransaction <- preparedTransactionP.toRight(
        RequestValidationErrors.MissingField
          .Reject("prepared_transaction")
          .asGrpcError
      )
      partySignaturesP <- requirePresence(partySignaturesOP, "parties_signatures")
      partySignatures <- validatePartySignatures(partySignaturesP)
      version <- validateHashingSchemeVersion(hashingSchemeVersionP).leftMap(_.asGrpcError)
      synchronizerIdString <- requirePresence(
        preparedTransactionP.flatMap(_.metadata.map(_.synchronizerId)),
        "synchronizer_id",
      )
      synchronizerId <- validateSynchronizerId(synchronizerIdString).leftMap(_.asGrpcError)
      ledgerEffectiveTime <- commandsValidator.validateLedgerTime(
        currentLedgerTime,
        minLedgerTimeP.flatMap(_.time.minLedgerTimeAbs),
        minLedgerTimeP.flatMap(_.time.minLedgerTimeRel),
      )
    } yield {
      ExecuteRequest(
        userId,
        submissionId,
        deduplicationPeriod,
        partySignatures,
        preparedTransaction,
        version,
        synchronizerId,
        ledgerEffectiveTime,
      )
    }
  }

  private def validateSynchronizerId(string: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[RpcError, SynchronizerId] =
    SynchronizerId
      .fromString(string)
      .leftMap(err =>
        RequestValidationErrors.InvalidField
          .Reject("synchronizer_id", err)
      )

  private def validateHashingSchemeVersion(protoVersion: iss.HashingSchemeVersion)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[RpcError, HashingSchemeVersion] = protoVersion match {
    case iss.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2 => Right(V2)
    case iss.HashingSchemeVersion.HASHING_SCHEME_VERSION_UNSPECIFIED =>
      Left(
        RequestValidationErrors.InvalidField
          .Reject("hashing_scheme_version", "Unspecified version")
      )
    case iss.HashingSchemeVersion.Unrecognized(unrecognizedValue) =>
      Left(
        RequestValidationErrors.InvalidField
          .Reject("hashing_scheme_version", s"Unrecognized version $unrecognizedValue")
      )
  }

  def validateReassignment(
      req: SubmitReassignmentRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, submission.SubmitReassignmentRequest] =
    for {
      commands <- requirePresence(req.reassignmentCommands, "reassignment_commands")
      submitReassignmentRequest <- commandsValidator.validateReassignmentCommands(
        commands
      )
    } yield submitReassignmentRequest

}
