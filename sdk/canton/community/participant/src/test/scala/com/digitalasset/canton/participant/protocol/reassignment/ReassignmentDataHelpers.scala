// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerCryptoPureApi}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
  UnassignmentData,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import java.util.UUID

final case class ReassignmentDataHelpers(
    contract: ContractInstance,
    sourceSynchronizer: Source[PhysicalSynchronizerId],
    targetSynchronizer: Target[PhysicalSynchronizerId],
    pureCrypto: SynchronizerCryptoPureApi,
    // mediatorCryptoClient and sequencerCryptoClient need to be defined for computation of the DeliveredUnassignmentResult
    mediatorCryptoClient: Option[SynchronizerCryptoClient] = None,
    sequencerCryptoClient: Option[SynchronizerCryptoClient] = None,
    targetTimestamp: Target[CantonTimestamp] = Target(CantonTimestamp.Epoch),
) {

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
  )(
      reassigningParticipants: Set[ParticipantId] = Set(submittingParticipant)
  ): UnassignmentRequest =
    UnassignmentRequest(
      submitterMetadata = submitterInfo(submitter, submittingParticipant),
      reassigningParticipants = reassigningParticipants,
      contracts = ContractsReassignmentBatch(contract, ReassignmentCounter(1)),
      sourceSynchronizer = sourceSynchronizer,
      sourceMediator = sourceMediator,
      targetSynchronizer = targetSynchronizer,
      targetTimestamp = targetTimestamp,
    )

  def unassignmentData(
      unassignmentRequest: UnassignmentRequest,
      unassignmentTs: CantonTimestamp = CantonTimestamp.Epoch,
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
      unassignmentRequest = fullUnassignmentViewTree,
      unassignmentTs = unassignmentTs,
    )
  }
}

object ReassignmentDataHelpers {

  def apply(
      contract: ContractInstance,
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      identityFactory: TestingIdentityFactory,
      targetTimestamp: Target[CantonTimestamp],
  ): ReassignmentDataHelpers = {
    val pureCrypto = identityFactory
      .forOwnerAndSynchronizer(DefaultTestIdentities.mediatorId, sourceSynchronizer.unwrap)
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
            sourceSynchronizer.unwrap,
          )
      ),
      sequencerCryptoClient = Some(
        identityFactory
          .forOwnerAndSynchronizer(
            DefaultTestIdentities.sequencerId,
            sourceSynchronizer.unwrap,
          )
      ),
      targetTimestamp = targetTimestamp,
    )
  }

  def apply(
      contract: ContractInstance,
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      identityFactory: TestingIdentityFactory,
  ): ReassignmentDataHelpers =
    apply(
      contract,
      sourceSynchronizer,
      targetSynchronizer,
      identityFactory,
      Target(CantonTimestamp.Epoch),
    )
}
