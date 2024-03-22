// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.{LfInterfaceId, LfPartyId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration
import scala.math.Ordered.orderingToOrdered

object GeneratorsData {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*
  import org.scalatest.EitherValues.*
  import org.scalatest.OptionValues.*

  private val tenYears: Duration = Duration.ofDays(365 * 10)

  implicit val cantonTimestampArb: Arbitrary[CantonTimestamp] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds * 1000 * 1000).map(CantonTimestamp.ofEpochMicro)
  )
  implicit val cantonTimestampSecondArb: Arbitrary[CantonTimestampSecond] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds).map(CantonTimestampSecond.ofEpochSecond)
  )

  implicit val viewPositionArb: Arbitrary[ViewPosition] = Arbitrary(
    Gen.listOf(merklePathElementArg.arbitrary).map(ViewPosition(_))
  )

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

  implicit val commonMetadataArb: Arbitrary[CommonMetadata] = Arbitrary(
    for {
      confirmationPolicy <- Arbitrary.arbitrary[ConfirmationPolicy]
      domainId <- Arbitrary.arbitrary[DomainId]

      mediatorRef <- Arbitrary.arbitrary[MediatorRef]
      singleMediatorRef <- Arbitrary.arbitrary[MediatorRef.Single]

      salt <- Arbitrary.arbitrary[Salt]
      uuid <- Gen.uuid
      protocolVersion <- representativeProtocolVersionGen(CommonMetadata)

      updatedMediatorRef =
        if (CommonMetadata.shouldHaveSingleMediator(protocolVersion)) singleMediatorRef
        else mediatorRef

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata
      .create(hashOps, protocolVersion)(
        confirmationPolicy,
        domainId,
        updatedMediatorRef,
        salt,
        uuid,
      )
      .value
  )

  implicit val participantMetadataArb: Arbitrary[ParticipantMetadata] = Arbitrary(
    for {
      ledgerTime <- Arbitrary.arbitrary[CantonTimestamp]
      submissionTime <- Arbitrary.arbitrary[CantonTimestamp]
      workflowIdO <- Gen.option(workflowIdArb.arbitrary)
      salt <- Arbitrary.arbitrary[Salt]

      protocolVersion <- representativeProtocolVersionGen(ParticipantMetadata)

      hashOps = TestHash // Not used for serialization
    } yield ParticipantMetadata(hashOps)(
      ledgerTime,
      submissionTime,
      workflowIdO,
      salt,
      protocolVersion.representative,
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
      protocolVersion <- representativeProtocolVersionGen(SubmitterMetadata)
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
      protocolVersion.representative,
    )
  )

  implicit val viewCommonDataArb: Arbitrary[ViewCommonData] = Arbitrary(for {
    informees <- Gen.containerOf[Set, Informee](Arbitrary.arbitrary[Informee])
    threshold <- Arbitrary.arbitrary[NonNegativeInt]
    salt <- Arbitrary.arbitrary[Salt]

    /*
      In v0 (used for pv=3,4) the confirmation policy is passed as part of the context rather than
      being read from the proto. This break the identify test in SerializationDeserializationTest.
      Fixing the test tooling currently requires too much energy compared to the benefit so filtering
      out the old protocol versions.
     */
    pv <- Gen.oneOf(ProtocolVersion.supported.forgetNE.filterNot(_.v < 5))

    hashOps = TestHash // Not used for serialization
  } yield ViewCommonData.create(hashOps)(informees, threshold, salt, pv))

  private def createActionDescriptionGenFor(
      pv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[CreateActionDescription] =
    for {
      contractId <- Arbitrary.arbitrary[LfContractId]
      seed <- Arbitrary.arbitrary[LfHash]
      version <- Arbitrary.arbitrary[LfTransactionVersion]
    } yield CreateActionDescription(contractId, seed, version)(pv)

  private def exerciseActionDescriptionGenFor(
      pv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[ExerciseActionDescription] =
    for {
      inputContractId <- Arbitrary.arbitrary[LfContractId]

      templateId <-
        if (pv >= ExerciseActionDescription.templateIdSupportedSince)
          Gen.option(Arbitrary.arbitrary[LfTemplateId])
        else Gen.const(None)

      choice <- Arbitrary.arbitrary[LfChoiceName]

      interfaceId <-
        if (pv >= ExerciseActionDescription.interfaceSupportedSince)
          Gen.option(Arbitrary.arbitrary[LfInterfaceId])
        else Gen.const(None)

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
      chosenValue,
      actors,
      byKey,
      seed,
      version,
      failed,
      pv,
    )

  private def fetchActionDescriptionGenFor(
      pv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[FetchActionDescription] =
    for {
      inputContractId <- Arbitrary.arbitrary[LfContractId]
      actors <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      byKey <- Gen.oneOf(true, false)
      version <- Arbitrary.arbitrary[LfTransactionVersion]
    } yield FetchActionDescription(inputContractId, actors, byKey, version)(pv)

  private def lookupByKeyActionDescriptionGenFor(
      pv: RepresentativeProtocolVersion[ActionDescription.type]
  ): Gen[LookupByKeyActionDescription] =
    for {
      key <- Arbitrary.arbitrary[LfGlobalKey]
      version <- Arbitrary.arbitrary[LfTransactionVersion]
    } yield LookupByKeyActionDescription.tryCreate(key, version, pv)

  // If this pattern match is not exhaustive anymore, update the method below
  {
    ((_: ActionDescription) match {
      case _: CreateActionDescription => ()
      case _: ExerciseActionDescription => ()
      case _: FetchActionDescription => ()
      case _: LookupByKeyActionDescription => ()
    }).discard
  }
  def actionDescriptionGenFor(pv: ProtocolVersion): Gen[ActionDescription] = {
    val rpv = ActionDescription.protocolVersionRepresentativeFor(pv)

    Gen.oneOf(
      createActionDescriptionGenFor(rpv),
      exerciseActionDescriptionGenFor(rpv),
      fetchActionDescriptionGenFor(rpv),
      lookupByKeyActionDescriptionGenFor(rpv),
    )
  }

  implicit val actionDescriptionArb: Arbitrary[ActionDescription] = Arbitrary(for {
    pv <- representativeProtocolVersionGen(ActionDescription)
    actionDescription <- actionDescriptionGenFor(pv.representative)
  } yield actionDescription)

  private def assignedKeyGen(cid: LfContractId): Gen[AssignedKey] =
    Arbitrary.arbitrary[LfTransactionVersion].map(AssignedKey(cid)(_))

  private implicit val freeKeyArb: Arbitrary[FreeKey] = Arbitrary(for {
    maintainers <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
    version <- Arbitrary.arbitrary[LfTransactionVersion]
  } yield FreeKey(maintainers)(version))

  implicit val viewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary(
    for {
      pv <- Arbitrary.arbitrary[ProtocolVersion]

      coreInputs <- Gen
        .listOf(
          Gen.zip(
            GeneratorsProtocol.serializableContractArb(canHaveEmptyKey = false, Some(pv)).arbitrary,
            Gen.oneOf(true, false),
          )
        )
        .map(_.map(InputContract.apply tupled))

      createdCore <- Gen
        .listOf(
          Gen.zip(
            GeneratorsProtocol.serializableContractArb(canHaveEmptyKey = false, Some(pv)).arbitrary,
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
        val key = contract.contract.metadata.maybeKey.value

        Gen.zip(key, assignedKeyGen(contract.contractId))
      })
      freeResolvedKeys <- Gen.listOf(
        Gen.zip(Arbitrary.arbitrary[LfGlobalKey], Arbitrary.arbitrary[FreeKey])
      )

      resolvedKeys = assignedResolvedKeys ++ freeResolvedKeys
      actionDescription <- actionDescriptionGenFor(pv)
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
      pv,
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
