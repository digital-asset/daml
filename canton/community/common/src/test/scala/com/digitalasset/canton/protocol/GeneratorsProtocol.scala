// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.transaction.Versioned
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{GeneratorsDataTime, ViewPosition}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.{DomainId, MediatorId, MediatorRef}
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsProtocol(
    protocolVersion: ProtocolVersion,
    generatorsDataTime: GeneratorsDataTime,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.{nonNegativeFiniteDurationArb as _, *}
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import org.scalatest.EitherValues.*
  import generatorsDataTime.*

  implicit val staticDomainParametersArb: Arbitrary[StaticDomainParameters] = {
    Arbitrary(for {
      requiredSigningKeySchemes <- nonEmptySetGen[SigningKeyScheme]
      requiredEncryptionKeySchemes <- nonEmptySetGen[EncryptionKeyScheme]
      requiredSymmetricKeySchemes <- nonEmptySetGen[SymmetricKeyScheme]
      requiredHashAlgorithms <- nonEmptySetGen[HashAlgorithm]
      requiredCryptoKeyFormats <- nonEmptySetGen[CryptoKeyFormat]

      parameters = StaticDomainParameters.create(
        requiredSigningKeySchemes,
        requiredEncryptionKeySchemes,
        requiredSymmetricKeySchemes,
        requiredHashAlgorithms,
        requiredCryptoKeyFormats,
        protocolVersion,
      )

    } yield parameters)
  }

  implicit val dynamicDomainParametersArb: Arbitrary[DynamicDomainParameters] = Arbitrary(
    for {
      participantResponseTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      mediatorReactionTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      transferExclusivityTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      topologyChangeDelay <- Arbitrary.arbitrary[NonNegativeFiniteDuration]

      mediatorDeduplicationMargin <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      // Because of the potential multiplication by 2 below, we want a reasonably small value
      ledgerTimeRecordTimeTolerance <- Gen
        .choose(0L, 10000L)
        .map(NonNegativeFiniteDuration.tryOfMicros)

      representativePV = DynamicDomainParameters.protocolVersionRepresentativeFor(protocolVersion)

      reconciliationInterval <- Arbitrary.arbitrary[PositiveSeconds]
      maxRatePerParticipant <- Arbitrary.arbitrary[NonNegativeInt]
      maxRequestSize <- Arbitrary.arbitrary[MaxRequestSize]

      trafficControlConfig <- Gen.option(Arbitrary.arbitrary[TrafficControlParameters])

      updatedMediatorDeduplicationTimeout = ledgerTimeRecordTimeTolerance * NonNegativeInt
        .tryCreate(2) + mediatorDeduplicationMargin

      sequencerAggregateSubmissionTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      onboardingRestriction <- Arbitrary.arbitrary[OnboardingRestriction]

      dynamicDomainParameters = DynamicDomainParameters.tryCreate(
        participantResponseTimeout,
        mediatorReactionTimeout,
        transferExclusivityTimeout,
        topologyChangeDelay,
        ledgerTimeRecordTimeTolerance,
        updatedMediatorDeduplicationTimeout,
        reconciliationInterval,
        maxRatePerParticipant,
        maxRequestSize,
        sequencerAggregateSubmissionTimeout,
        trafficControlConfig,
        onboardingRestriction,
      )(representativePV)

    } yield dynamicDomainParameters
  )

  implicit val rootHashArb: Arbitrary[RootHash] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(RootHash(_))
  )
  implicit val viewHashArb: Arbitrary[ViewHash] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(ViewHash(_))
  )

  implicit val confirmationPolicyArb: Arbitrary[ConfirmationPolicy] = genArbitrary

  implicit val serializableRawContractInstanceArb: Arbitrary[SerializableRawContractInstance] =
    Arbitrary(
      for {
        agreementText <- Gen.asciiPrintableStr.map(AgreementText(_))
        contractInstance = ExampleTransactionFactory.contractInstance()
      } yield SerializableRawContractInstance.create(contractInstance, agreementText).value
    )

  private lazy val unicumGenerator: UnicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())

  {
    // If this pattern match is not exhaustive anymore, update the method below
    ((_: CantonContractIdVersion) match {
      case AuthenticatedContractIdVersionV2 => ()
    }).discard
  }
  def serializableContractArb(
      canHaveEmptyKey: Boolean
  ): Arbitrary[SerializableContract] = {
    val contractIdVersion = AuthenticatedContractIdVersionV2

    Arbitrary(
      for {
        rawContractInstance <- Arbitrary.arbitrary[SerializableRawContractInstance]
        metadata <- contractMetadataArb(canHaveEmptyKey).arbitrary
        ledgerCreateTime <- Arbitrary.arbitrary[LedgerCreateTime]

        domainId <- Arbitrary.arbitrary[DomainId]
        mediatorId <- Arbitrary.arbitrary[MediatorId]

        saltIndex <- Gen.choose(Int.MinValue, Int.MaxValue)
        transactionUUID <- Gen.uuid

        (computedSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
          domainId = domainId,
          mediator = MediatorRef(mediatorId),
          transactionUuid = transactionUUID,
          viewPosition = ViewPosition(List.empty),
          viewParticipantDataSalt = TestSalt.generateSalt(saltIndex),
          createIndex = 0,
          ledgerCreateTime = ledgerCreateTime,
          metadata = metadata,
          suffixedContractInstance = rawContractInstance,
          contractIdVersion = contractIdVersion,
        )

        index <- Gen.posNum[Int]
        contractIdDiscriminator = ExampleTransactionFactory.lfHash(index)

        contractId = contractIdVersion.fromDiscriminator(
          contractIdDiscriminator,
          unicum,
        )

        contractSalt = if (contractIdVersion.isAuthenticated) Some(computedSalt.unwrap) else None

      } yield SerializableContract(
        contractId,
        rawContractInstance,
        metadata,
        ledgerCreateTime,
        contractSalt,
      )
    )
  }

  implicit val globalKeyWithMaintainersArb: Arbitrary[Versioned[LfGlobalKeyWithMaintainers]] =
    Arbitrary(
      for {
        maintainers <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
        key <- Arbitrary.arbitrary[LfGlobalKey]
      } yield ExampleTransactionFactory.globalKeyWithMaintainers(
        key,
        maintainers,
      )
    )

  def contractMetadataArb(canHaveEmptyKey: Boolean): Arbitrary[ContractMetadata] = Arbitrary(
    for {
      maybeKeyWithMaintainers <-
        if (canHaveEmptyKey) Gen.option(globalKeyWithMaintainersArb.arbitrary)
        else Gen.some(globalKeyWithMaintainersArb.arbitrary)
      maintainers = maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.unversioned.maintainers)

      signatories <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      observers <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])

      allSignatories = maintainers ++ signatories
      allStakeholders = allSignatories ++ observers

      // Required invariant: maintainers \subset signatories \subset stakeholders
    } yield ContractMetadata.tryCreate(
      signatories = allSignatories,
      stakeholders = allStakeholders,
      maybeKeyWithMaintainers,
    )
  )

  implicit val requestIdArb: Arbitrary[RequestId] = genArbitrary

  implicit val rollbackContextArb: Arbitrary[RollbackContext] =
    Arbitrary(Gen.listOf(Arbitrary.arbitrary[PositiveInt]).map(RollbackContext.apply))

  implicit val createdContractArb: Arbitrary[CreatedContract] = Arbitrary(
    for {
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
      consumedInCore <- Gen.oneOf(true, false)
      rolledBack <- Gen.oneOf(true, false)
    } yield CreatedContract
      .create(
        contract,
        consumedInCore,
        rolledBack,
      )
      .value
  )
}
