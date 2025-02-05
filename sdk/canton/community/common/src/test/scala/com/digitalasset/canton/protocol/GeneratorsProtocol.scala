// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.pruning.CounterParticipantIntervalsBehind
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.transaction.ParticipantSynchronizerLimits
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.transaction.Versioned
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsProtocol(
    protocolVersion: ProtocolVersion
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import org.scalatest.EitherValues.*

  implicit val staticSynchronizerParametersArb: Arbitrary[StaticSynchronizerParameters] =
    Arbitrary(for {
      requiredSigningAlgorithmSpecs <- nonEmptySetGen[SigningAlgorithmSpec]
      requiredSigningKeySpecs <- nonEmptySetGen[SigningKeySpec]
      requiredEncryptionAlgorithmSpecs <- nonEmptySetGen[EncryptionAlgorithmSpec]
      requiredEncryptionKeySpecs <- nonEmptySetGen[EncryptionKeySpec]
      requiredSymmetricKeySchemes <- nonEmptySetGen[SymmetricKeyScheme]
      requiredHashAlgorithms <- nonEmptySetGen[HashAlgorithm]
      requiredCryptoKeyFormats <- nonEmptySetGen[CryptoKeyFormat]
      requiredSignatureFormats <- nonEmptySetGen[SignatureFormat]

      parameters = StaticSynchronizerParameters(
        RequiredSigningSpecs(requiredSigningAlgorithmSpecs, requiredSigningKeySpecs),
        RequiredEncryptionSpecs(requiredEncryptionAlgorithmSpecs, requiredEncryptionKeySpecs),
        requiredSymmetricKeySchemes,
        requiredHashAlgorithms,
        requiredCryptoKeyFormats,
        requiredSignatureFormats,
        protocolVersion,
      )

    } yield parameters)

  implicit val dynamicSynchronizerParametersArb: Arbitrary[DynamicSynchronizerParameters] =
    Arbitrary(
      for {
        confirmationResponseTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        mediatorReactionTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        assignmentExclusivityTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        topologyChangeDelay <- Arbitrary.arbitrary[NonNegativeFiniteDuration]

        mediatorDeduplicationMargin <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        // Because of the potential multiplication by 2 below, we want a reasonably small value
        ledgerTimeRecordTimeTolerance <- Gen
          .choose(0L, 10000L)
          .map(NonNegativeFiniteDuration.tryOfMicros)

        representativePV = DynamicSynchronizerParameters.protocolVersionRepresentativeFor(
          protocolVersion
        )

        reconciliationInterval <- Arbitrary.arbitrary[PositiveSeconds]
        maxRequestSize <- Arbitrary.arbitrary[MaxRequestSize]

        trafficControlConfig <- Gen.option(Arbitrary.arbitrary[TrafficControlParameters])

        updatedMediatorDeduplicationTimeout = ledgerTimeRecordTimeTolerance * NonNegativeInt
          .tryCreate(2) + mediatorDeduplicationMargin

        sequencerAggregateSubmissionTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        onboardingRestriction <- Arbitrary.arbitrary[OnboardingRestriction]

        participantSynchronizerLimits <- Arbitrary.arbitrary[ParticipantSynchronizerLimits]

        acsCommitmentsCatchupConfig <-
          for {
            isNone <- Gen.oneOf(true, false)
            skip <- Gen.choose(1, Math.sqrt(PositiveInt.MaxValue.value.toDouble).intValue)
            trigger <- Gen.choose(1, Math.sqrt(PositiveInt.MaxValue.value.toDouble).intValue)
          } yield {
            if (!isNone)
              Some(
                new AcsCommitmentsCatchUpParameters(
                  PositiveInt.tryCreate(skip),
                  PositiveInt.tryCreate(trigger),
                )
              )
            else None
          }

        // Because of the potential multiplication by 2 below, we want a reasonably small value
        submissionTimeRecordTimeTolerance <- Gen
          .choose(0L, 10000L)
          .map(NonNegativeFiniteDuration.tryOfMicros)

        dynamicSynchronizerParameters = DynamicSynchronizerParameters.tryCreate(
          confirmationResponseTimeout,
          mediatorReactionTimeout,
          assignmentExclusivityTimeout,
          topologyChangeDelay,
          ledgerTimeRecordTimeTolerance,
          updatedMediatorDeduplicationTimeout,
          reconciliationInterval,
          maxRequestSize,
          sequencerAggregateSubmissionTimeout,
          trafficControlConfig,
          onboardingRestriction,
          acsCommitmentsCatchupConfig,
          participantSynchronizerLimits,
          submissionTimeRecordTimeTolerance,
        )(representativePV)

      } yield dynamicSynchronizerParameters
    )

  implicit val counterParticipantIntervalsBehindArb: Arbitrary[CounterParticipantIntervalsBehind] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        participantId <- Arbitrary.arbitrary[ParticipantId]
        intervalsBehind <- Arbitrary.arbitrary[NonNegativeLong]
        timeBehind <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        asOfSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      } yield CounterParticipantIntervalsBehind(
        synchronizerId,
        participantId,
        intervalsBehind,
        timeBehind,
        asOfSequencingTime,
      )
    )

  implicit val dynamicSequencingParametersArb: Arbitrary[DynamicSequencingParameters] = Arbitrary(
    for {
      payload <- Arbitrary.arbitrary[Option[ByteString]]
      representativePV = DynamicSequencingParameters.protocolVersionRepresentativeFor(
        protocolVersion
      )
      dynamicSequencingParameters = DynamicSequencingParameters(payload)(representativePV)
    } yield dynamicSequencingParameters
  )

  implicit val rootHashArb: Arbitrary[RootHash] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(RootHash(_))
  )
  implicit val viewHashArb: Arbitrary[ViewHash] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(ViewHash(_))
  )

  implicit val serializableRawContractInstanceArb: Arbitrary[SerializableRawContractInstance] = {
    val contractInstance = ExampleTransactionFactory.contractInstance()
    Arbitrary(SerializableRawContractInstance.create(contractInstance).value)
  }

  private lazy val unicumGenerator: UnicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())

  {
    // If this pattern match is not exhaustive anymore, update the method below
    ((_: CantonContractIdVersion) match {
      case AuthenticatedContractIdVersionV10 => ()
    }).discard
  }
  def serializableContractArb(
      canHaveEmptyKey: Boolean
  ): Arbitrary[SerializableContract] = {
    val contractIdVersion = AuthenticatedContractIdVersionV10

    Arbitrary(
      for {
        rawContractInstance <- Arbitrary.arbitrary[SerializableRawContractInstance]
        metadata <- contractMetadataArb(canHaveEmptyKey).arbitrary
        ledgerCreateTime <- Arbitrary.arbitrary[LedgerCreateTime]

        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        mediatorGroup <- Arbitrary.arbitrary[MediatorGroupRecipient]

        saltIndex <- Gen.choose(Int.MinValue, Int.MaxValue)
        transactionUUID <- Gen.uuid

        (computedSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
          synchronizerId = synchronizerId,
          mediator = mediatorGroup,
          transactionUuid = transactionUUID,
          viewPosition = ViewPosition(List.empty),
          viewParticipantDataSalt = TestSalt.generateSalt(saltIndex),
          createIndex = 0,
          ledgerCreateTime = ledgerCreateTime,
          metadata = metadata,
          suffixedContractInstance = rawContractInstance,
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

  implicit val stakeholdersArb: Arbitrary[Stakeholders] = Arbitrary(
    for {
      signatories <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      observers <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
    } yield Stakeholders.withSignatoriesAndObservers(
      signatories = signatories,
      observers = observers,
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

  implicit val externalAuthorizationArb: Arbitrary[ExternalAuthorization] = Arbitrary(
    for {
      signatures <- Arbitrary.arbitrary[Map[PartyId, Seq[Signature]]]
      hashingSchemeVersion <- Arbitrary.arbitrary[HashingSchemeVersion]
    } yield ExternalAuthorization.create(signatures, hashingSchemeVersion, protocolVersion)
  )

  implicit val protocolSymmetricKeyArb: Arbitrary[ProtocolSymmetricKey] =
    Arbitrary(
      for {
        key <- Arbitrary.arbitrary[SymmetricKey]
      } yield ProtocolSymmetricKey(key, protocolVersion)
    )

}
