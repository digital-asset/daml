// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.either.*
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.{LfTemplateId, Stakeholders}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Checks that need to be performed as part of phase 3 of the unassignment request processing
  */
private[reassignment] final case class UnassignmentValidation(
    expectedStakeholders: Stakeholders,
    expectedTemplateId: LfTemplateId,
    sourceProtocolVersion: Source[ProtocolVersion],
    sourceTopology: Source[TopologySnapshot],
    // Defined if and only if the participant is observing reassigning
    targetTopology: Option[Target[TopologySnapshot]],
    recipients: Recipients,
    engine: DAMLe,
    loggerFactory: NamedLoggerFactory,
)(request: FullUnassignmentTree)(implicit ec: ExecutionContext, traceContext: TraceContext) {
  private def checkParticipants: EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    targetTopology match {
      case Some(targetTopology) =>
        UnassignmentValidationReassigningParticipant(
          expectedStakeholders = expectedStakeholders,
          expectedTemplateId,
          sourceProtocolVersion,
          sourceTopology,
          targetTopology,
          recipients,
        )(request)
      case None => EitherT.pure(())
    }
}

// TODO(#22119) Move validations in the case class or remove the case class
private[reassignment] object UnassignmentValidation {

  /** @param targetTopology Defined if and only if the participant is observing reassigning
    */
  def perform(
      serializableContractAuthenticator: SerializableContractAuthenticator,
      sourceProtocolVersion: Source[ProtocolVersion],
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      recipients: Recipients,
      engine: DAMLe,
      getEngineAbortStatus: GetEngineAbortStatus,
      loggerFactory: NamedLoggerFactory,
  )(request: FullUnassignmentTree)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val metadataCheckF = new ReassignmentValidation(engine, loggerFactory)
      .checkMetadata(request, getEngineAbortStatus)
      .mapK(FutureUnlessShutdown.outcomeK)

    val authenticationCheckF = EitherT.fromEither[FutureUnlessShutdown](
      serializableContractAuthenticator
        .authenticate(request.contract)
        .leftMap[ReassignmentProcessorError](ContractError.apply)
    )

    for {
      _ <- metadataCheckF
      _ <- authenticationCheckF

      // Now that the contract and metadata are validated, this is safe to use
      expectedStakeholders = Stakeholders(request.contract.metadata)
      expectedTemplateId =
        request.contract.rawContractInstance.contractInstance.unversioned.template

      validation = UnassignmentValidation(
        expectedStakeholders = expectedStakeholders,
        expectedTemplateId,
        sourceProtocolVersion,
        sourceTopology,
        targetTopology,
        recipients,
        engine,
        loggerFactory,
      )(request)

      _ <- ReassignmentValidation
        .checkSubmitter(
          ReassignmentRef(request.contractId),
          topologySnapshot = sourceTopology,
          submitter = request.submitter,
          participantId = request.submitterMetadata.submittingParticipant,
          stakeholders = expectedStakeholders.all,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- validation.checkParticipants
    } yield ()
  }

}
