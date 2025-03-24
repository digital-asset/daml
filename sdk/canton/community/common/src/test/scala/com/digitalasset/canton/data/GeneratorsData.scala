// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{GeneratorsCrypto, Salt, SigningKeyUsage, TestHash}
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
  LookupByKeyActionDescription,
}
import com.digitalasset.canton.data.MerkleTree.VersionedMerkleTree
import com.digitalasset.canton.data.ViewPosition.{MerklePathElement, MerkleSeqIndex}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResultMessage,
  DeliveredUnassignmentResult,
  SignedProtocolMessage,
  Verdict,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, MediatorGroupRecipient, SignedContent}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SeqUtil
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.{LfInterfaceId, LfPackageId, LfPartyId, LfVersioned}
import com.digitalasset.daml.lf.value.Value.ValueInt64
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.EitherValues.*

import scala.util.Random

final class GeneratorsData(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*
  import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsProtocol.*
  import org.scalatest.OptionValues.*

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: MerklePathElement) match {
      case _: ViewPosition.MerkleSeqIndex => ()
      case _: ViewPosition.MerkleSeqIndexFromRoot =>
        () // This one is excluded because it is not made to be serialized
    }).discard
  }
  implicit val merklePathElementArg: Arbitrary[MerklePathElement] = Arbitrary(
    Arbitrary.arbitrary[MerkleSeqIndex]
  )

  implicit val viewPositionArb: Arbitrary[ViewPosition] = Arbitrary(
    Gen.listOf(merklePathElementArg.arbitrary).map(ViewPosition(_))
  )

  implicit val commonMetadataArb: Arbitrary[CommonMetadata] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]

      mediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      salt <- Arbitrary.arbitrary[Salt]
      uuid <- Gen.uuid

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata
      .create(hashOps, protocolVersion)(
        synchronizerId,
        mediator,
        salt,
        uuid,
      )
  )

  implicit val participantMetadataArb: Arbitrary[ParticipantMetadata] = Arbitrary(
    for {
      ledgerTime <- Arbitrary.arbitrary[CantonTimestamp]
      submissionTime <- Arbitrary.arbitrary[CantonTimestamp]
      workflowIdO <- Gen.option(workflowIdArb.arbitrary)
      salt <- Arbitrary.arbitrary[Salt]

      hashOps = TestHash // Not used for serialization
    } yield ParticipantMetadata(hashOps)(
      ledgerTime,
      submissionTime,
      workflowIdO,
      salt,
      protocolVersion,
    )
  )

  implicit val submitterMetadataArb: Arbitrary[SubmitterMetadata] = Arbitrary(
    for {
      actAs <- nonEmptySet(lfPartyIdArb).arbitrary
      userId <- userIdArb.arbitrary
      commandId <- commandIdArb.arbitrary
      submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
      salt <- Arbitrary.arbitrary[Salt]
      submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
      dedupPeriod <- Arbitrary.arbitrary[DeduplicationPeriod]
      maxSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      externalAuthorization <- Gen.option(Arbitrary.arbitrary[ExternalAuthorization])
    } yield SubmitterMetadata(
      actAs,
      userId,
      commandId,
      submittingParticipant,
      salt,
      submissionId,
      dedupPeriod,
      maxSequencingTime,
      externalAuthorization,
      hashOps = TestHash, // Not used for serialization
      protocolVersion,
    )
  )

  implicit val viewConfirmationParametersArb: Arbitrary[ViewConfirmationParameters] = Arbitrary(
    for {
      informees <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      viewConfirmationParameters <-
        Gen
          .containerOf[Seq, Quorum](Arbitrary.arbitrary[Quorum](quorumArb(informees.toSeq)))
          .map(ViewConfirmationParameters.tryCreate(informees, _))
    } yield viewConfirmationParameters
  )

  def quorumArb(informees: Seq[LfPartyId]): Arbitrary[Quorum] = Arbitrary(
    for {
      confirmersWeights <- Gen
        .containerOfN[Seq, PositiveInt](informees.size, Arbitrary.arbitrary[PositiveInt])

      random = new Random()
      shuffledInformees = SeqUtil.randomSubsetShuffle(
        informees.toIndexedSeq,
        informees.size,
        random,
      )

      confirmers = shuffledInformees.zip(confirmersWeights).toMap
      threshold <- Arbitrary.arbitrary[NonNegativeInt]
    } yield Quorum(confirmers, threshold)
  )

  implicit val viewCommonDataArb: Arbitrary[ViewCommonData] = Arbitrary(
    for {
      viewConfirmationParameters <- Arbitrary.arbitrary[ViewConfirmationParameters]
      salt <- Arbitrary.arbitrary[Salt]
      hashOps = TestHash // Not used for serialization
    } yield ViewCommonData.tryCreate(hashOps)(
      viewConfirmationParameters,
      salt,
      protocolVersion,
    )
  )

  private def createActionDescriptionGenFor(
      rpv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[CreateActionDescription] =
    for {
      contractId <- Arbitrary.arbitrary[LfContractId]
      seed <- Arbitrary.arbitrary[LfHash]
    } yield CreateActionDescription(contractId, seed)(rpv)

  private def exerciseActionDescriptionGenFor(
      rpv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[ExerciseActionDescription] =
    for {
      inputContractId <- Arbitrary.arbitrary[LfContractId]

      templateId <- Arbitrary.arbitrary[LfTemplateId]

      choice <- Arbitrary.arbitrary[LfChoiceName]

      interfaceId <- Gen.option(Arbitrary.arbitrary[LfInterfaceId])

      packagePreference <- Gen.containerOf[Set, LfPackageId](Arbitrary.arbitrary[LfPackageId])

      // We consider only this specific value because the goal is not exhaustive testing of LF (de)serialization
      chosenValue <- Gen.long.map(ValueInt64.apply)
      version <- Arbitrary.arbitrary[LfLanguageVersion]

      actors <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      seed <- Arbitrary.arbitrary[LfHash]
      byKey <- Gen.oneOf(true, false)
      failed <- Gen.oneOf(true, false)

    } yield ExerciseActionDescription.tryCreate(
      inputContractId,
      templateId,
      choice,
      interfaceId,
      packagePreference,
      LfVersioned(version, chosenValue),
      actors,
      byKey,
      seed,
      failed,
      rpv,
    )

  private def fetchActionDescriptionGenFor(
      rpv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[FetchActionDescription] =
    for {
      inputContractId <- Arbitrary.arbitrary[LfContractId]
      actors <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      byKey <- Gen.oneOf(true, false)
      templateId <- Arbitrary.arbitrary[LfTemplateId]
      interfaceId <- Gen.option(Arbitrary.arbitrary[LfInterfaceId])
    } yield FetchActionDescription(inputContractId, actors, byKey, templateId, interfaceId)(rpv)

  private def lookupByKeyActionDescriptionGenFor(
      rpv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[LookupByKeyActionDescription] =
    for {
      key <- Arbitrary.arbitrary[LfVersioned[LfGlobalKey]]
    } yield LookupByKeyActionDescription.tryCreate(key, rpv)

  // If this pattern match is not exhaustive anymore, update the method below
  {
    ((_: ActionDescription) match {
      case _: CreateActionDescription => ()
      case _: ExerciseActionDescription => ()
      case _: FetchActionDescription => ()
      case _: LookupByKeyActionDescription => ()
    }).discard
  }

  implicit val actionDescriptionArb: Arbitrary[ActionDescription] = Arbitrary {
    val rpv = ActionDescription.protocolVersionRepresentativeFor(protocolVersion)

    Gen.oneOf(
      createActionDescriptionGenFor(rpv),
      exerciseActionDescriptionGenFor(rpv),
      fetchActionDescriptionGenFor(rpv),
      lookupByKeyActionDescriptionGenFor(rpv),
    )
  }

  private implicit val freeKeyArb: Arbitrary[FreeKey] = Arbitrary(for {
    maintainers <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
  } yield FreeKey(maintainers))

  implicit val viewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary(
    for {
      actionDescription <- actionDescriptionArb.arbitrary

      coreInputs <- actionDescription match {
        case ex: ExerciseActionDescription =>
          for {
            c <- Gen
              .zip(
                generatorsProtocol
                  .serializableContractArb(canHaveEmptyKey = false)
                  .arbitrary
                  .map(_.copy(contractId = ex.inputContractId)),
                Gen.oneOf(true, false),
              )
              .map(InputContract.apply tupled)

            others <- Gen
              .listOf(
                Gen.zip(
                  generatorsProtocol.serializableContractArb(canHaveEmptyKey = false).arbitrary,
                  Gen.oneOf(true, false),
                )
              )
              .map(_.map(InputContract.apply tupled))
          } yield (c +: others).groupBy(_.contractId).flatMap { case (_, contracts) =>
            contracts.headOption
          }

        case fetch: FetchActionDescription =>
          generatorsProtocol
            .serializableContractArb(canHaveEmptyKey = false)
            .arbitrary
            .map(c =>
              List(InputContract(c.copy(contractId = fetch.inputContractId), consumed = false))
            )
        case _: CreateActionDescription | _: LookupByKeyActionDescription => Gen.const(List.empty)
      }

      createdCore <- actionDescription match {
        case created: CreateActionDescription =>
          Gen
            .zip(
              generatorsProtocol
                .serializableContractArb(canHaveEmptyKey = false)
                .arbitrary,
              Gen.oneOf(true, false),
            )
            .map { case (c, rolledBack) =>
              List(
                CreatedContract.tryCreate(
                  c.copy(contractId = created.contractId),
                  consumedInCore = false,
                  rolledBack = rolledBack,
                )
              )
            }

        case _: ExerciseActionDescription =>
          Gen
            .listOf(
              Gen.zip(
                generatorsProtocol.serializableContractArb(canHaveEmptyKey = false).arbitrary,
                Gen.oneOf(true, false),
                Gen.oneOf(true, false),
              )
            )
            .map(_.map(CreatedContract.tryCreate tupled))
            // Deduplicating on contract id
            .map(
              _.groupBy(_.contract.contractId).flatMap { case (_, contracts) =>
                contracts.headOption
              }
            )

        case _: LookupByKeyActionDescription | _: FetchActionDescription => Gen.const(List.empty)
      }

      notTransient = (createdCore.map(_.contract.contractId) ++ coreInputs.map(_.contractId)).toSet

      createdInSubviewArchivedInCore <- Gen
        .containerOf[Set, LfContractId](
          Arbitrary.arbitrary[LfContractId]
        )
        // createdInSubviewArchivedInCore and notTransient should be disjoint
        .map(_ -- notTransient)

      /*
        Resolved keys
        AssignedKey must correspond to a contract in core input
       */
      coreInputWithResolvedKeys <- Gen.someOf(coreInputs)
      assignedResolvedKeys <- Gen.sequence[List[
        (LfGlobalKey, LfVersioned[SerializableKeyResolution])
      ], (LfGlobalKey, LfVersioned[SerializableKeyResolution])](coreInputWithResolvedKeys.map {
        contract =>
          // Unsafe .value is fine because we force the key to be defined with the generator above
          val key = contract.contract.metadata.maybeKeyWithMaintainersVersioned.value
          Gen
            .zip(key, AssignedKey(contract.contractId))
            .map { case (LfVersioned(v, k), r) => (k.globalKey, LfVersioned(v, r)) }
      })
      freeResolvedKeys <- actionDescription match {
        case _: CreateActionDescription | _: FetchActionDescription => Gen.const(List.empty)

        case _: ExerciseActionDescription =>
          Gen.listOf(
            Gen
              .zip(Arbitrary.arbitrary[LfGlobalKey], Arbitrary.arbitrary[LfVersioned[FreeKey]])
          )

        case LookupByKeyActionDescription(key) =>
          Arbitrary.arbitrary[LfVersioned[FreeKey]].map(res => List(key.unversioned -> res))
      }

      resolvedKeys = assignedResolvedKeys ++ freeResolvedKeys
      rollbackContext <- Arbitrary.arbitrary[RollbackContext]
      salt <- Arbitrary.arbitrary[Salt]

      hashOps = TestHash // Not used for serialization
    } yield ViewParticipantData.tryCreate(hashOps)(
      coreInputs.map(contract => (contract.contractId, contract)).toMap,
      createdCore.toSeq,
      createdInSubviewArchivedInCore,
      resolvedKeys.toMap,
      actionDescription,
      rollbackContext,
      salt,
      protocolVersion,
    )
  )

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: ViewType) match {
      case ViewType.TransactionViewType => ()
      case _: ViewType.ReassignmentViewType => ()
      case _: ViewTypeTest => () // Only for tests, so we don't use it in the generator
    }).discard
  }
  implicit val viewTypeArb: Arbitrary[ViewType] = Arbitrary(
    Gen.oneOf[ViewType](
      ViewType.TransactionViewType,
      ViewType.AssignmentViewType,
      ViewType.UnassignmentViewType,
    )
  )

  private val transactionViewWithEmptyTransactionSubviewArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      emptySubviews = TransactionSubviews.empty(
        protocolVersion,
        hashOps,
      ) // empty TransactionSubviews
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData,
      viewParticipantData =
        viewParticipantData.blindFully, // The view participant data in an informee tree must be blinded
      subviews = emptySubviews,
      protocolVersion,
    )
  )

  implicit val transactionViewArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      transactionViewWithEmptySubview <-
        transactionViewWithEmptyTransactionSubviewArb.arbitrary
      subviews = TransactionSubviews
        .apply(Seq(transactionViewWithEmptySubview))(protocolVersion, hashOps)
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData,
      viewParticipantData = viewParticipantData,
      subviews = subviews,
      protocolVersion,
    )
  )

  private val transactionViewForInformeeTreeArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      transactionViewWithEmptySubview <-
        transactionViewWithEmptyTransactionSubviewArb.arbitrary
      subviews = TransactionSubviews
        .apply(Seq(transactionViewWithEmptySubview))(protocolVersion, hashOps)
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData,
      viewParticipantData =
        viewParticipantData.blindFully, // The view participant data in an informee tree must be blinded
      subviews = subviews,
      protocolVersion,
    )
  )

  implicit val fullInformeeTreeArb: Arbitrary[FullInformeeTree] = Arbitrary(
    for {
      submitterMetadata <- submitterMetadataArb.arbitrary
      commonData <- commonMetadataArb.arbitrary
      participantData <- participantMetadataArb.arbitrary
      rootViews <- transactionViewForInformeeTreeArb.arbitrary
      hashOps = TestHash
      rootViewsMerkleSeq = MerkleSeq.fromSeq(hashOps, protocolVersion)(Seq(rootViews))
      genTransactionTree = GenTransactionTree
        .tryCreate(hashOps)(
          submitterMetadata,
          commonData,
          participantData.blindFully, // The view participant data in an informee tree must be blinded
          rootViews = rootViewsMerkleSeq,
        )
    } yield FullInformeeTree.tryCreate(tree = genTransactionTree, protocolVersion)
  )

  // here we want to test the (de)serialization of the MerkleSeq and we use SubmitterMetadata as the VersionedMerkleTree.
  // other VersionedMerkleTree types are tested in their respective tests
  implicit val merkleSeqArb: Arbitrary[MerkleSeq[VersionedMerkleTree[?]]] =
    Arbitrary(
      for {
        submitterMetadataSeq <- Gen.listOf(submitterMetadataArb.arbitrary)
      } yield MerkleSeq.fromSeq(TestHash, protocolVersion)(submitterMetadataSeq)
    )

  private val sourceProtocolVersion = Source(protocolVersion)
  private val targetProtocolVersion = Target(protocolVersion)

  implicit val reassignmentSubmitterMetadataArb: Arbitrary[ReassignmentSubmitterMetadata] =
    Arbitrary(
      for {
        submitter <- Arbitrary.arbitrary[LfPartyId]
        userId <- userIdArb.arbitrary.map(_.unwrap)
        submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
        commandId <- commandIdArb.arbitrary.map(_.unwrap)
        submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
        workflowId <- Gen.option(workflowIdArb.arbitrary.map(_.unwrap))

      } yield ReassignmentSubmitterMetadata(
        submitter,
        submittingParticipant,
        commandId,
        submissionId,
        userId,
        workflowId,
      )
    )

  implicit val assignmentCommonDataArb: Arbitrary[AssignmentCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      targetSynchronizerId <- Arbitrary.arbitrary[Target[SynchronizerId]]

      targetMediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      stakeholders <- Arbitrary.arbitrary[Stakeholders]

      uuid <- Gen.uuid

      submitterMetadata <- Arbitrary.arbitrary[ReassignmentSubmitterMetadata]
      reassigningParticipants <- Arbitrary.arbitrary[Set[ParticipantId]]

      hashOps = TestHash // Not used for serialization

    } yield AssignmentCommonData
      .create(hashOps)(
        salt,
        targetSynchronizerId,
        targetMediator,
        stakeholders,
        uuid,
        submitterMetadata,
        targetProtocolVersion,
        reassigningParticipants,
      )
  )

  implicit val unassignmentCommonData: Arbitrary[UnassignmentCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      sourceSynchronizerId <- Arbitrary.arbitrary[Source[SynchronizerId]]

      sourceMediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      stakeholders <- Arbitrary.arbitrary[Stakeholders]
      reassigningParticipants <- Arbitrary.arbitrary[Set[ParticipantId]]

      uuid <- Gen.uuid

      submitterMetadata <- Arbitrary.arbitrary[ReassignmentSubmitterMetadata]

      hashOps = TestHash // Not used for serialization

    } yield UnassignmentCommonData
      .create(hashOps)(
        salt,
        sourceSynchronizerId,
        sourceMediator,
        stakeholders,
        reassigningParticipants,
        uuid,
        submitterMetadata,
        sourceProtocolVersion,
      )
  )

  private def deliveryUnassignmentResultGen(
      sourceProtocolVersion: Source[ProtocolVersion]
  ): Gen[DeliveredUnassignmentResult] =
    for {
      sourceSynchronizerId <- Arbitrary.arbitrary[Source[SynchronizerId]]
      requestId <- Arbitrary.arbitrary[RequestId]
      rootHash <- Arbitrary.arbitrary[RootHash]
      protocolVersion = sourceProtocolVersion.unwrap
      verdict = Verdict.Approve(protocolVersion)

      result = ConfirmationResultMessage.create(
        sourceSynchronizerId.unwrap,
        ViewType.UnassignmentViewType,
        requestId,
        rootHash,
        verdict,
        protocolVersion,
      )

      signedResult =
        SignedProtocolMessage.from(
          result,
          protocolVersion,
          GeneratorsCrypto.sign(
            GeneratorsCrypto.testSigningKey.fingerprint,
            "UnassignmentResult-mediator",
            TestHash.testHashPurpose,
            SigningKeyUsage.ProtocolOnly,
          ),
        )

      recipients <- recipientsArb.arbitrary

      batch = Batch.of(protocolVersion, signedResult -> recipients)
      deliver <- deliverGen(sourceSynchronizerId.unwrap, batch, protocolVersion)

      unassignmentTs <- Arbitrary.arbitrary[CantonTimestamp]
    } yield DeliveredUnassignmentResult
      .create(
        SignedContent(
          deliver,
          sign(
            GeneratorsCrypto.testSigningKey.fingerprint,
            "UnassignmentResult-sequencer",
            TestHash.testHashPurpose,
            SigningKeyUsage.ProtocolOnly,
          ),
          Some(unassignmentTs),
          protocolVersion,
        )
      )
      .value

  implicit val assignmentViewArb: Arbitrary[AssignmentView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
      unassignmentResultEvent <- deliveryUnassignmentResultGen(sourceProtocolVersion)
      reassignmentCounter <- reassignmentCounterGen

      hashOps = TestHash // Not used for serialization

    } yield AssignmentView
      .create(hashOps)(
        salt,
        contract,
        unassignmentResultEvent,
        targetProtocolVersion,
        reassignmentCounter,
      )
      .value
  )

  implicit val unassignmentViewArb: Arbitrary[UnassignmentView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]

      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary

      targetSynchronizerId <- Arbitrary.arbitrary[Target[SynchronizerId]]
      timeProof <- timeProofArb(protocolVersion).arbitrary
      reassignmentCounter <- reassignmentCounterGen

      hashOps = TestHash // Not used for serialization

    } yield UnassignmentView
      .create(hashOps)(
        salt,
        contract,
        targetSynchronizerId,
        timeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
        reassignmentCounter,
      )
  )

  implicit val assignViewTreeArb: Arbitrary[AssignmentViewTree] = Arbitrary(
    for {
      commonData <- assignmentCommonDataArb.arbitrary
      assignmentView <- assignmentViewArb.arbitrary
      hash = TestHash
    } yield AssignmentViewTree(
      commonData,
      assignmentView.blindFully,
      Target(protocolVersion),
      hash,
    )
  )

  implicit val unassignmentViewTreeArb: Arbitrary[UnassignmentViewTree] = Arbitrary(
    for {
      commonData <- unassignmentCommonData.arbitrary
      unassignmentView <- unassignmentViewArb.arbitrary
      hash = TestHash
    } yield UnassignmentViewTree(
      commonData,
      unassignmentView.blindFully,
      Source(protocolVersion),
      hash,
    )
  )

  private val fullyBlindedTransactionViewWithEmptyTransactionSubviewArb
      : Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      emptySubviews = TransactionSubviews.empty(
        protocolVersion,
        hashOps,
      ) // empty TransactionSubviews
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData.blindFully,
      viewParticipantData = viewParticipantData.blindFully,
      subviews = emptySubviews.blindFully,
      protocolVersion,
    )
  )

  private var unblindedSubviewHashesForLightTransactionTree: Seq[ViewHashAndKey] = _

  private val transactionViewForLightTransactionTreeArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      transactionViewWithEmptySubview <-
        fullyBlindedTransactionViewWithEmptyTransactionSubviewArb.arbitrary
      subviews = TransactionSubviews
        .apply(Seq(transactionViewWithEmptySubview))(protocolVersion, hashOps)
      subviewHashes = subviews.trySubviewHashes
      pureCrypto = ExampleTransactionFactory.pureCrypto
      subviewHashesAndKeys = subviewHashes.map { hash =>
        ViewHashAndKey(
          hash,
          pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes),
        )
      }
    } yield {
      unblindedSubviewHashesForLightTransactionTree = subviewHashesAndKeys
      TransactionView.tryCreate(hashOps)(
        viewCommonData = viewCommonData,
        viewParticipantData = viewParticipantData,
        subviews =
          subviews.blindFully, // only a single view in a LightTransactionTree can be unblinded
        protocolVersion,
      )
    }
  )

  implicit val lightTransactionViewTreeArb: Arbitrary[LightTransactionViewTree] = Arbitrary(
    for {
      submitterMetadata <- submitterMetadataArb.arbitrary
      commonData <- commonMetadataArb.arbitrary
      participantData <- participantMetadataArb.arbitrary
      rootViews <- transactionViewForLightTransactionTreeArb.arbitrary
      hashOps = TestHash
      rootViewsMerkleSeq = MerkleSeq.fromSeq(hashOps, protocolVersion)(Seq(rootViews))
      genTransactionTree = GenTransactionTree
        .tryCreate(hashOps)(
          submitterMetadata,
          commonData,
          participantData,
          rootViews = rootViewsMerkleSeq,
        )
    } yield LightTransactionViewTree.tryCreate(
      tree = genTransactionTree,
      unblindedSubviewHashesForLightTransactionTree,
      protocolVersion,
    )
  )

}
