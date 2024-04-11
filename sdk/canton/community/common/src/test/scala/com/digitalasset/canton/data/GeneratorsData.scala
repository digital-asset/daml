// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

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
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.{LfInterfaceId, LfPackageId, LfPartyId, LfVersioned}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

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
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsProtocol.*
  import org.scalatest.OptionValues.*

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

      mediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      salt <- Arbitrary.arbitrary[Salt]
      uuid <- Gen.uuid

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata
      .create(hashOps, protocolVersion)(
        confirmationPolicy,
        domainId,
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
      applicationId <- applicationIdArb.arbitrary
      commandId <- commandIdArb.arbitrary
      submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
      salt <- Arbitrary.arbitrary[Salt]
      submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
      dedupPeriod <- Arbitrary.arbitrary[DeduplicationPeriod]
      maxSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      hashOps = TestHash // Not used for serialization
    } yield SubmitterMetadata(
      actAs,
      applicationId,
      commandId,
      submittingParticipant,
      salt,
      submissionId,
      dedupPeriod,
      maxSequencingTime,
      hashOps,
      protocolVersion,
    )
  )

  implicit val viewCommonDataArb: Arbitrary[ViewCommonData] = Arbitrary(for {
    informees <- Gen.containerOf[Set, Informee](Arbitrary.arbitrary[Informee])
    threshold <- Arbitrary.arbitrary[NonNegativeInt]
    salt <- Arbitrary.arbitrary[Salt]
    hashOps = TestHash // Not used for serialization
  } yield ViewCommonData.create(hashOps)(informees, threshold, salt, protocolVersion))

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
      chosenValue <- Gen.long.map(ValueInt64)
      version <- Arbitrary.arbitrary[LfTransactionVersion]

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
    } yield FetchActionDescription(inputContractId, actors, byKey)(rpv)

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

      coreInputs <- Gen
        .listOf(
          Gen.zip(
            generatorsProtocol.serializableContractArb(canHaveEmptyKey = false).arbitrary,
            Gen.oneOf(true, false),
          )
        )
        .map(_.map(InputContract.apply tupled))

      createdCore <- Gen
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
        (LfGlobalKey, LfVersioned[SerializableKeyResolution])
      ], (LfGlobalKey, LfVersioned[SerializableKeyResolution])](coreInputWithResolvedKeys.map {
        contract =>
          // Unsafe .value is fine because we force the key to be defined with the generator above
          val key = contract.contract.metadata.maybeKeyWithMaintainersVersioned.value
          Gen
            .zip(key, AssignedKey(contract.contractId))
            .map({ case (LfVersioned(v, k), r) => (k.globalKey, LfVersioned(v, r)) })
      })
      freeResolvedKeys <- Gen.listOf(
        Gen
          .zip(Arbitrary.arbitrary[LfGlobalKey], Arbitrary.arbitrary[LfVersioned[FreeKey]])
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
