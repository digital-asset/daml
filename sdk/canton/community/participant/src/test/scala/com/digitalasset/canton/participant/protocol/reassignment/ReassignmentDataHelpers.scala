// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerCryptoPureApi}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID

final case class ReassignmentDataHelpers(
    contract: SerializableContract,
    sourceSynchronizer: Source[PhysicalSynchronizerId],
    targetSynchronizer: Target[PhysicalSynchronizerId],
    pureCrypto: SynchronizerCryptoPureApi,
    // mediatorCryptoClient and sequencerCryptoClient need to be defined for computation of the DeliveredUnassignmentResult
    mediatorCryptoClient: Option[SynchronizerCryptoClient] = None,
    sequencerCryptoClient: Option[SynchronizerCryptoClient] = None,
    targetTime: CantonTimestamp = CantonTimestamp.Epoch,
) {

  private val targetTimeProof: TimeProof = TimeProofTestUtil.mkTimeProof(
    timestamp = targetTime,
    targetSynchronizer = targetSynchronizer,
  )

  private val seedGenerator: SeedGenerator =
    new SeedGenerator(pureCrypto)

  private def submitterInfo(
      submitter: LfPartyId,
      submittingParticipant: ParticipantId,
  ): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("assignment-validation-command-id"),
      submissionId = None,
      LedgerUserId.assertFromString("tests"),
      workflowId = None,
    )

  def unassignmentRequest(
      submitter: LfPartyId,
      submittingParticipant: ParticipantId,
      sourceMediator: MediatorGroupRecipient,
      sourceProtocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion,
  )(
      reassigningParticipants: Set[ParticipantId] = Set(submittingParticipant)
  ): UnassignmentRequest =
    UnassignmentRequest(
      submitterMetadata = submitterInfo(submitter, submittingParticipant),
      reassigningParticipants = reassigningParticipants,
      contracts = ContractsReassignmentBatch(contract, ReassignmentCounter(1)),
      sourceSynchronizer = sourceSynchronizer,
      sourceProtocolVersion = Source(sourceProtocolVersion),
      sourceMediator = sourceMediator,
      targetSynchronizer = targetSynchronizer,
      targetTimeProof = targetTimeProof,
    )

  def unassignmentData(
      reassignmentId: ReassignmentId,
      unassignmentRequest: UnassignmentRequest,
  ): UnassignmentData = {
    val uuid = new UUID(10L, 0L)
    val seed = seedGenerator.generateSaltSeed()

    val fullUnassignmentViewTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    UnassignmentData(
      reassignmentId = reassignmentId,
      unassignmentRequest = fullUnassignmentViewTree,
    )
  }
}

object ReassignmentDataHelpers {

  def apply(
      contract: SerializableContract,
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      identityFactory: TestingIdentityFactory,
  ) = {
    val pureCrypto = identityFactory
      .forOwnerAndSynchronizer(DefaultTestIdentities.mediatorId, sourceSynchronizer.unwrap.logical)
      .pureCrypto

    new ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = sourceSynchronizer,
      targetSynchronizer = targetSynchronizer,
      pureCrypto = pureCrypto,
      mediatorCryptoClient = Some(
        identityFactory
          .forOwnerAndSynchronizer(
            DefaultTestIdentities.mediatorId,
            sourceSynchronizer.unwrap.logical,
          )
      ),
      sequencerCryptoClient = Some(
        identityFactory
          .forOwnerAndSynchronizer(
            DefaultTestIdentities.sequencerId,
            sourceSynchronizer.unwrap.logical,
          )
      ),
    )
  }
}
