// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value.ValueInt64
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{Salt, TestHash}
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
  LookupByKeyActionDescription,
}
import com.digitalasset.canton.data.ViewPosition.{MerklePathElement, MerkleSeqIndex}
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.protocol.{
  ConfirmationPolicy,
  CreatedContract,
  GeneratorsProtocol,
  InputContract,
  LfChoiceName,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfTemplateId,
  LfTransactionVersion,
  RollbackContext,
}
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.{LfInterfaceId, LfPackageId, LfPartyId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.math.Ordered.orderingToOrdered

final class GeneratorsData(
    protocolVersion: ProtocolVersion,
    generatorsDataTime: GeneratorsDataTime,
    generatorsProtocol: GeneratorsProtocol,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*
  import org.scalatest.OptionValues.*
  import generatorsProtocol.*
  import generatorsDataTime.*

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: MerklePathElement) match {
      case _: ViewPosition.ListIndex =>
        () // This one is excluded because it is not made to be serialized
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
      confirmationPolicy <- Arbitrary.arbitrary[ConfirmationPolicy]
      domainId <- Arbitrary.arbitrary[DomainId]

      mediatorRef <- Arbitrary.arbitrary[MediatorRef]

      salt <- Arbitrary.arbitrary[Salt]
      uuid <- Gen.uuid

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata(hashOps, protocolVersion)(
      confirmationPolicy,
      domainId,
      mediatorRef,
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
      applicationId <- applicationIdArb.arbitrary
      commandId <- commandIdArb.arbitrary
      submitterParticipant <- Arbitrary.arbitrary[ParticipantId]
      salt <- Arbitrary.arbitrary[Salt]
      submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
      dedupPeriod <- Arbitrary.arbitrary[DeduplicationPeriod]
      maxSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      hashOps = TestHash // Not used for serialization
    } yield SubmitterMetadata(
      actAs,
      applicationId,
      commandId,
      submitterParticipant,
      salt,
      submissionId,
      dedupPeriod,
      maxSequencingTime,
      hashOps,
      protocolVersion,
    )
  )

  implicit val viewConfirmationParametersArb: Arbitrary[ViewConfirmationParameters] = Arbitrary(
    for {
      informees <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      viewConfirmationParameters <-
        if (protocolVersion <= ProtocolVersion.v5) {
          Arbitrary
            .arbitrary[Quorum](quorumArb(informees.toSeq))
            .map(quorum => ViewConfirmationParameters.tryCreate(informees, Seq(quorum)))
        } else
          Gen
            .containerOf[Seq, Quorum](Arbitrary.arbitrary[Quorum](quorumArb(informees.toSeq)))
            .map(ViewConfirmationParameters.tryCreate(informees, _))
    } yield viewConfirmationParameters
  )

  def quorumArb(informees: Seq[LfPartyId]): Arbitrary[Quorum] = Arbitrary(
    for {
      confirmers <- Gen.sequence[Set[ConfirmingParty], ConfirmingParty](
        informees.map(pId => Arbitrary.arbitrary[ConfirmingParty].map(cp => cp.copy(party = pId)))
      )
      threshold <- Arbitrary.arbitrary[NonNegativeInt]
    } yield Quorum.create(confirmers, threshold)
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
      version <- Arbitrary.arbitrary[LfTransactionVersion]
    } yield CreateActionDescription(contractId, seed, version)(rpv)

  private def exerciseActionDescriptionGenFor(
      rpv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[ExerciseActionDescription] =
    for {
      inputContractId <- Arbitrary.arbitrary[LfContractId]

      templateId <-
        if (rpv >= ExerciseActionDescription.templateIdSupportedSince)
          Gen.option(Arbitrary.arbitrary[LfTemplateId])
        else Gen.const(None)

      choice <- Arbitrary.arbitrary[LfChoiceName]

      interfaceId <-
        if (rpv >= ExerciseActionDescription.interfaceSupportedSince)
          Gen.option(Arbitrary.arbitrary[LfInterfaceId])
        else Gen.const(None)

      packagePreference <-
        if (rpv >= ExerciseActionDescription.packagePreferenceSupportedSince)
          Gen.containerOf[Set, LfPackageId](Arbitrary.arbitrary[LfPackageId])
        else Gen.const(Set.empty[LfPackageId])

      // We consider only this specific value because the goal is not exhaustive testing of LF (de)serialization
      chosenValue <- Gen.long.map(ValueInt64)

      actors <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      seed <- Arbitrary.arbitrary[LfHash]
      version <- Arbitrary.arbitrary[LfTransactionVersion]
      byKey <- Gen.oneOf(true, false)
      failed <- Gen.oneOf(true, false)

    } yield ExerciseActionDescription.tryCreate(
      inputContractId,
      templateId,
      choice,
      interfaceId,
      packagePreference,
      chosenValue,
      actors,
      byKey,
      seed,
      version,
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
      version <- Arbitrary.arbitrary[LfTransactionVersion]
    } yield FetchActionDescription(inputContractId, actors, byKey, version)(rpv)

  private def lookupByKeyActionDescriptionGenFor(
      rpv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[LookupByKeyActionDescription] =
    for {
      vk <- Arbitrary.arbitrary[Versioned[LfGlobalKey]]
    } yield LookupByKeyActionDescription.tryCreate(vk.unversioned, vk.version, rpv)

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

  private def assignedKeyGen(cid: LfContractId): Gen[AssignedKey] =
    Arbitrary.arbitrary[LfTransactionVersion].map(AssignedKey(cid)(_))

  private implicit val freeKeyArb: Arbitrary[FreeKey] = Arbitrary(for {
    maintainers <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
    version <- Arbitrary.arbitrary[LfTransactionVersion]
  } yield FreeKey(maintainers)(version))

  implicit val viewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary(
    for {

      coreInputs <- Gen
        .listOf(
          Gen.zip(
            generatorsProtocol
              .serializableContractArb(canHaveEmptyKey = false, Some(protocolVersion))
              .arbitrary,
            Gen.oneOf(true, false),
          )
        )
        .map(_.map(InputContract.apply tupled))

      createdCore <- Gen
        .listOf(
          Gen.zip(
            generatorsProtocol
              .serializableContractArb(canHaveEmptyKey = false, Some(protocolVersion))
              .arbitrary,
            Gen.oneOf(true, false),
            Gen.oneOf(true, false),
          )
        )
        .map(_.map(CreatedContract.tryCreate tupled))
        // Deduplicating on contract id
        .map(
          _.groupBy(_.contract.contractId).flatMap { case (_, contracts) => contracts.headOption }
        )

      createdCoreIds = createdCore.map(_.contract.contractId).toSet

      createdInSubviewArchivedInCore <- Gen
        .containerOf[Set, LfContractId](
          Arbitrary.arbitrary[LfContractId]
        )
        // createdInSubviewArchivedInCore and createdCore should be disjoint
        .map(_.intersect(createdCoreIds))

      /*
        Resolved keys
        AssignedKey must correspond to a contract in core input
       */
      coreInputWithResolvedKeys <- Gen.someOf(coreInputs)

      assignedResolvedKeys <- Gen.sequence[List[
        (LfGlobalKey, SerializableKeyResolution)
      ], (LfGlobalKey, SerializableKeyResolution)](coreInputWithResolvedKeys.map { contract =>
        // Unsafe .value is fine because we force the key to be defined with the generator above
        val key = contract.contract.metadata.maybeKeyWithMaintainersVersioned.value
        Gen
          .zip(key, assignedKeyGen(contract.contractId))
          .map({ case (k, r) => (k.unversioned.globalKey, r.copy()(k.version)) })
      })

      freeResolvedKeys <- Gen.listOf(
        Gen
          .zip(Arbitrary.arbitrary[Versioned[LfGlobalKey]], Arbitrary.arbitrary[FreeKey])
          .map({ case (k, r) => (k.unversioned, r.copy()(k.version)) })
      )

      resolvedKeys = assignedResolvedKeys ++ freeResolvedKeys
      actionDescription <- actionDescriptionArb.arbitrary
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
      case _: ViewType.TransferViewType => ()
      case _: ViewTypeTest => () // Only for tests, so we don't use it in the generator
    }).discard
  }
  implicit val viewTypeArb: Arbitrary[ViewType] = Arbitrary(
    Gen.oneOf[ViewType](
      ViewType.TransactionViewType,
      ViewType.TransferInViewType,
      ViewType.TransferOutViewType,
    )
  )

}
