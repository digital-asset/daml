// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.either.*
import com.digitalasset.canton.data.{FullUnassignmentTree, ReassignmentRef}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

private[reassignment] object UnassignmentValidation {

  /** @param targetTopology Defined if and only if the participant is reassigning
    */
  def perform(
      serializableContractAuthenticator: SerializableContractAuthenticator,
      sourceProtocolVersion: Source[ProtocolVersion],
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      recipients: Recipients,
  )(request: FullUnassignmentTree)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val authenticationCheckF = EitherT.fromEither[FutureUnlessShutdown](
      serializableContractAuthenticator
        .authenticate(request.contract)
        .leftMap[ReassignmentProcessorError](ContractError.apply)
    )

    for {
      _ <- authenticationCheckF

      // Now that the contract and metadata are validated, this is safe to use
      expectedStakeholders = Stakeholders(request.contract.metadata)
      expectedTemplateId =
        request.contract.rawContractInstance.contractInstance.unversioned.template

      _ <- ReassignmentValidation
        .checkSubmitter(
          ReassignmentRef(request.contractId),
          topologySnapshot = sourceTopology,
          submitter = request.submitter,
          participantId = request.submitterMetadata.submittingParticipant,
          stakeholders = expectedStakeholders.all,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <- targetTopology match {
        case Some(targetTopology) =>
          UnassignmentValidationReassigningParticipant(
            expectedStakeholders = expectedStakeholders,
            expectedTemplateId,
            sourceProtocolVersion,
            sourceTopology,
            targetTopology,
            recipients,
          )(request)
        case None => EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())
      }
    } yield ()
  }

}
