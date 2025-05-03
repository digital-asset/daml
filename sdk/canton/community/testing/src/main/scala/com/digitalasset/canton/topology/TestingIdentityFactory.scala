// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.{
  defaultStaticSynchronizerParameters,
  testedReleaseProtocolVersion,
}
import com.digitalasset.canton.concurrent.{
  DirectExecutionContext,
  FutureSupervisor,
  HasFutureSupervision,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchingConfig,
  DefaultProcessingTimeouts,
  SessionSigningKeysConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  StaticSynchronizerParameters,
  SynchronizerParameters,
  TestSynchronizerParameters,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolverUS,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil}
import com.digitalasset.canton.{BaseTest, FutureHelpers, LfPackageId, LfPartyId}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

import TestingTopology.*

/** Utility functions to setup identity & crypto apis for testing purposes
  *
  * You are trying to figure out how to setup identity topologies and crypto apis to drive your unit
  * tests? Then YOU FOUND IT! The present file contains everything that you should need.
  *
  * First, let's re-call that we are abstracting the identity and crypto aspects from the
  * transaction protocol.
  *
  * Therefore, all the key crypto operations hide behind the so-called crypto-api which splits into
  * the pure part [[CryptoPureApi]] and the more complicated part, the [[SyncCryptoApi]] such that
  * from the transaction protocol perspective, we can conveniently use methods like
  * [[SyncCryptoApi.sign]] or [[SyncCryptoApi.encryptFor]]
  *
  * The abstraction creates the following hierarchy of classes to resolve the state for a given
  * [[Member]] on a per (synchronizerId, timestamp)
  *
  * SyncCryptoApiProvider - root object that makes the synchronisation topology state known to a
  * node accessible .forSynchronizer - method to get the specific view on a per synchronizer basis
  * \= SynchronizerSyncCryptoApi .snapshot(timestamp) | recentState - method to get the view for a
  * specific time \= SynchronizerSnapshotSyncCryptoApi (extends SyncCryptoApi)
  *
  * All these object carry the necessary objects ([[CryptoPureApi]], [[TopologySnapshot]],
  * [[KeyVaultApi]]) as arguments with them.
  *
  * Now, in order to conveniently create a static topology for testing, we provide a <ul>
  * <li>[[TestingTopology]] which allows us to define a certain static topology</li>
  * <li>[[TestingIdentityFactory]] which consumes a static topology and delivers all necessary
  * components and objects that a unit test might need.</li> <li>[[DefaultTestIdentities]] which
  * provides a predefined set of identities that can be used for unit tests.</li> </ul>
  *
  * Common usage patterns are: <ul> <li>Get a [[SynchronizerCryptoClient]] with an empty topology:
  * `TestingIdentityFactory().forOwnerAndSynchronizer(participant1)`</li> <li>To get a
  * [[SynchronizerSnapshotSyncCryptoApi]]: same as above, just add `.recentState`.</li> <li>Define a
  * specific topology and get the [[SyncCryptoApiProvider]]:
  * `TestingTopology().withTopology(Map(party1 -> participant1)).build()`.</li> </ul>
  *
  * @param synchronizers
  *   Set of synchronizers for which the topology is valid
  * @param topology
  *   Static association of parties to participants in the most complete way it can be defined in
  *   this testing class.
  * @param participants
  *   participants for which keys should be added. A participant mentioned in `topology` will be
  *   included automatically in the topology state, so such a participant does not need to be
  *   declared again. If a participant occurs both in `topology` and `participants`, the attributes
  *   of `participants` have higher precedence.
  * @param keyPurposes
  *   The purposes of the keys that will be generated.
  */
final case class TestingTopology(
    synchronizers: Set[SynchronizerId] = defaultSynchronizers,
    topology: Map[LfPartyId, PartyInfo] = Map.empty,
    mediatorGroups: Set[MediatorGroup] = defaultMediatorGroups,
    sequencerGroup: SequencerGroup = defaultSequencerGroup,
    participants: Map[ParticipantId, ParticipantAttributes] = Map.empty,
    packages: Map[ParticipantId, Seq[VettedPackage]] = Map.empty,
    keyPurposes: Set[KeyPurpose] = KeyPurpose.All,
    synchronizerParameters: List[
      SynchronizerParameters.WithValidity[DynamicSynchronizerParameters]
    ] = defaultSynchronizerParams,
    staticSynchronizerParameters: StaticSynchronizerParameters =
      defaultStaticSynchronizerParameters,
    freshKeys: AtomicBoolean = new AtomicBoolean(false),
    sessionSigningKeysConfig: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
) {
  def mediators: Seq[MediatorId] = mediatorGroups.toSeq.flatMap(_.all)

  /** Overwrites the `staticSynchronizerParameters` field.
    */
  def withStaticSynchronizerParams(
      staticSynchronizerParameters: StaticSynchronizerParameters
  ): TestingTopology =
    this.copy(staticSynchronizerParameters = staticSynchronizerParameters)

  /** Define for which synchronizers the topology should apply.
    *
    * All synchronizers will have exactly the same topology.
    */
  def withSynchronizers(synchronizers: SynchronizerId*): TestingTopology =
    this.copy(synchronizers = synchronizers.toSet)

  def withDynamicSynchronizerParameters(
      dynamicSynchronizerParameters: DynamicSynchronizerParameters,
      validFrom: CantonTimestamp = CantonTimestamp.Epoch,
  ): TestingTopology =
    copy(
      synchronizerParameters = List(
        SynchronizerParameters.WithValidity(
          validFrom = validFrom,
          validUntil = None,
          parameter = dynamicSynchronizerParameters,
        )
      )
    )

  /** Overwrites the `sequencerGroup` field.
    */
  def withSequencerGroup(
      sequencerGroup: SequencerGroup
  ): TestingTopology =
    this.copy(sequencerGroup = sequencerGroup)

  /** Overwrites the `participants` parameter while setting attributes to Submission / Ordinary.
    */
  def withSimpleParticipants(
      participants: ParticipantId*
  ): TestingTopology =
    this.copy(participants =
      participants
        .map(
          _ -> ParticipantAttributes(ParticipantPermission.Submission, None)
        )
        .toMap
    )

  /** Overwrites the `participants` parameter.
    */
  def withParticipants(
      participants: (ParticipantId, ParticipantAttributes)*
  ): TestingTopology =
    this.copy(participants = participants.toMap)

  def allParticipants(): Set[ParticipantId] =
    (topology.values
      .map(_.participants)
      .flatMap(x => x.keys) ++ participants.keys).toSet

  def withKeyPurposes(keyPurposes: Set[KeyPurpose]): TestingTopology =
    this.copy(keyPurposes = keyPurposes)

  def withFreshKeys(freshKeys: Boolean): TestingTopology =
    this.copy(freshKeys = new AtomicBoolean(freshKeys))

  /** Define the topology as a simple map of party to participant */
  def withTopology(
      parties: Map[LfPartyId, ParticipantId],
      permission: ParticipantPermission = ParticipantPermission.Submission,
  ): TestingTopology = {
    val topology: Map[LfPartyId, PartyInfo] = parties
      .fmap(_ -> permission)
      .groupMap(_._1)(_._2)
      .fmap(participants => toPartyInfo(participants.toMap))

    this.copy(topology = topology)
  }

  /** Define the topology as a map of participant to map of parties */
  def withReversedTopology(
      parties: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]]
  ): TestingTopology = {
    val converted = parties.toSeq
      .flatMap { case (participantId, partyToPermission) =>
        partyToPermission.toSeq.map { case (party, permission) =>
          (party, (participantId, permission))
        }
      }
      .groupMap(_._1)(_._2)
      .fmap(participants => toPartyInfo(participants.toMap))
    copy(topology = converted)
  }

  def withThreshold(
      parties: Map[LfPartyId, (PositiveInt, Seq[(ParticipantId, ParticipantPermission)])]
  ): TestingTopology = {
    val tmp: Map[LfPartyId, PartyInfo] = parties.map { case (party, (threshold, participants)) =>
      party -> PartyInfo(
        threshold,
        participants.map { case (pid, permission) =>
          pid -> ParticipantAttributes(permission, None)
        }.toMap,
      )
    }
    this.copy(topology = tmp)
  }

  def withPackages(packages: Map[ParticipantId, Seq[LfPackageId]]): TestingTopology =
    this.copy(packages = packages.view.mapValues(VettedPackage.unbounded).toMap)

  def build(
      loggerFactory: NamedLoggerFactory = NamedLoggerFactory("test-area", "crypto")
  ): TestingIdentityFactory =
    build(
      SymbolicCrypto.create(
        testedReleaseProtocolVersion,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      ),
      loggerFactory,
    )

  def build(
      crypto: Crypto,
      loggerFactory: NamedLoggerFactory,
  ): TestingIdentityFactory =
    new TestingIdentityFactory(
      this,
      crypto,
      loggerFactory,
      synchronizerParameters,
      staticSynchronizerParameters,
      sessionSigningKeysConfig,
    )
}

object TestingTopology {

  private val defaultSynchronizers: Set[SynchronizerId] = Set(DefaultTestIdentities.synchronizerId)
  private val defaultSequencerGroup: SequencerGroup = SequencerGroup(
    active = Seq(DefaultTestIdentities.sequencerId),
    passive = Seq.empty,
    threshold = PositiveInt.one,
  )
  private val defaultMediatorGroups: Set[MediatorGroup] = Set(
    MediatorGroup(
      NonNegativeInt.zero,
      Seq(DefaultTestIdentities.mediatorId),
      Seq(),
      PositiveInt.one,
    )
  )
  private val defaultSynchronizerParams = List(
    SynchronizerParameters.WithValidity(
      validFrom = CantonTimestamp.Epoch,
      validUntil = None,
      parameter = DefaultTestIdentities.defaultDynamicSynchronizerParameters,
    )
  )

  def from(
      synchronizers: Set[SynchronizerId] = defaultSynchronizers,
      topology: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] = Map.empty,
      mediatorGroups: Set[MediatorGroup] = defaultMediatorGroups,
      sequencerGroup: SequencerGroup = defaultSequencerGroup,
      participants: Map[ParticipantId, ParticipantAttributes] = Map.empty,
      packages: Map[ParticipantId, Seq[VettedPackage]] = Map.empty,
      synchronizerParameters: List[
        SynchronizerParameters.WithValidity[DynamicSynchronizerParameters]
      ] = defaultSynchronizerParams,
      sessionSigningKeysConfig: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
  ): TestingTopology =
    new TestingTopology(
      synchronizers = synchronizers,
      topology = topology.fmap(toPartyInfo),
      mediatorGroups = mediatorGroups,
      sequencerGroup = sequencerGroup,
      participants = participants,
      packages = packages,
      synchronizerParameters = synchronizerParameters,
      sessionSigningKeysConfig = sessionSigningKeysConfig,
    )

  private def toPartyInfo(participants: Map[ParticipantId, ParticipantPermission]): PartyInfo = {
    val participantAttributes = participants.map { case (participantId, permission) =>
      participantId -> ParticipantAttributes(permission, None)
    }
    PartyInfo(threshold = PositiveInt.one, participants = participantAttributes)
  }
}

class TestingIdentityFactory(
    topology: TestingTopology,
    val crypto: Crypto,
    override protected val loggerFactory: NamedLoggerFactory,
    dynamicSynchronizerParameters: List[
      SynchronizerParameters.WithValidity[DynamicSynchronizerParameters]
    ],
    staticSynchronizerParameters: StaticSynchronizerParameters =
      defaultStaticSynchronizerParameters,
    sessionSigningKeysConfig: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
) extends NamedLogging
    with FutureHelpers {
  protected implicit val directExecutionContext: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  @VisibleForTesting
  def getTopology(): TestingTopology = this.topology

  def forOwner(
      owner: Member,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
      currentSnapshotApproximationTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ): SyncCryptoApiParticipantProvider =
    new SyncCryptoApiParticipantProvider(
      owner,
      ips(availableUpToInclusive, currentSnapshotApproximationTimestamp),
      crypto,
      sessionSigningKeysConfig,
      BatchingConfig().parallelism,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )

  def forOwnerAndSynchronizer(
      owner: Member,
      synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
      currentSnapshotApproximationTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ): SynchronizerCryptoClient =
    forOwner(owner, availableUpToInclusive, currentSnapshotApproximationTimestamp)
      .tryForSynchronizer(
        synchronizerId,
        staticSynchronizerParameters,
      )

  private def ips(
      upToInclusive: CantonTimestamp,
      currentSnapshotApproximationTimestamp: CantonTimestamp,
  ): IdentityProvidingServiceClient = {
    val ips = new IdentityProvidingServiceClient()
    synchronizers.foreach(dId =>
      ips.add(new SynchronizerTopologyClient() with HasFutureSupervision with NamedLogging {

        override protected def loggerFactory: NamedLoggerFactory =
          TestingIdentityFactory.this.loggerFactory

        override protected def futureSupervisor: FutureSupervisor = FutureSupervisor.Noop

        override protected val executionContext: ExecutionContext =
          TestingIdentityFactory.this.directExecutionContext

        override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(
            implicit traceContext: TraceContext
        ): FutureUnlessShutdown[Boolean] = ???

        override def awaitUS(
            condition: TopologySnapshot => FutureUnlessShutdown[Boolean],
            timeout: Duration,
        )(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Boolean] = ???

        override def synchronizerId: SynchronizerId = dId

        override def trySnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): TopologySnapshot = {
          require(
            timestamp <= upToInclusive,
            s"Topology information not yet available for $timestamp",
          )
          topologySnapshot(synchronizerId, timestampForSynchronizerParameters = timestamp)
        }

        override def currentSnapshotApproximation(implicit
            traceContext: TraceContext
        ): TopologySnapshot =
          topologySnapshot(
            synchronizerId,
            timestampForSynchronizerParameters = currentSnapshotApproximationTimestamp,
            timestampOfSnapshot = currentSnapshotApproximationTimestamp,
          )

        override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
          timestamp <= upToInclusive

        override def awaitSequencedTimestamp(timestampInclusive: SequencedTime)(implicit
            traceContext: TraceContext
        ): Option[FutureUnlessShutdown[Unit]] =
          Option.when(timestampInclusive.value > upToInclusive) {
            ErrorUtil.internalErrorAsyncShutdown(
              new IllegalArgumentException(
                s"Attempt to wait for observing the sequenced time $timestampInclusive that will never be available"
              )
            )

          }

        override def awaitTimestamp(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Option[FutureUnlessShutdown[Unit]] =
          Option.when(timestamp > upToInclusive) {
            ErrorUtil.internalErrorAsyncShutdown(
              new IllegalArgumentException(
                s"Attempt to obtain a topology snapshot at $timestamp that will never be available"
              )
            )
          }

        override def approximateTimestamp: CantonTimestamp =
          currentSnapshotApproximation(TraceContext.empty).timestamp

        override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] =
          FutureUnlessShutdown.fromTry(Try(trySnapshot(timestamp)))

        override def close(): Unit = ()

        override def topologyKnownUntilTimestamp: CantonTimestamp = upToInclusive

        override def snapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] = awaitSnapshot(timestamp)

        override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
          FutureUnlessShutdown.pure(None)
      })
    )
    ips
  }

  private val defaultProtocolVersion = BaseTest.testedProtocolVersion

  private def synchronizers: Set[SynchronizerId] = topology.synchronizers

  def topologySnapshot(
      synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId,
      packageDependencyResolver: PackageDependencyResolverUS =
        StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
      timestampForSynchronizerParameters: CantonTimestamp = CantonTimestamp.Epoch,
      timestampOfSnapshot: CantonTimestamp = CantonTimestamp.Epoch,
  ): TopologySnapshotLoader = {

    val store = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      BaseTest.testedProtocolVersion,
      loggerFactory,
      DefaultProcessingTimeouts.testing,
    )

    // Compute default participant permissions to be the highest granted to an individual party
    val partyToParticipantPermission: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] =
      topology.topology.map { case (partyId, partyInfo) =>
        partyId -> partyInfo.participants.map { case (participantId, attributes) =>
          participantId -> attributes.permission
        }
      }
    val defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission] =
      partyToParticipantPermission.foldLeft(Map.empty[ParticipantId, ParticipantPermission]) {
        case (acc, (_, permissionByParticipant)) =>
          MapsUtil.extendedMapWith(acc, permissionByParticipant)(ParticipantPermission.higherOf)
      }

    val participantTxs =
      participantsTxs(defaultPermissionByParticipant, topology.packages)

    val synchronizerMembers =
      (topology.sequencerGroup.active ++ topology.sequencerGroup.passive ++ topology.mediators)
        .flatMap(m => genKeyCollection(m))

    val mediatorOnboarding = topology.mediatorGroups.map(group =>
      mkAdd(
        MediatorSynchronizerState
          .create(
            synchronizerId,
            group = group.index,
            threshold = group.threshold,
            active = group.active,
            observers = group.passive,
          )
          .getOrElse(sys.error("creating MediatorSynchronizerState should not have failed"))
      )
    )

    val sequencerOnboarding =
      mkAdd(
        SequencerSynchronizerState
          .create(
            synchronizerId,
            threshold = topology.sequencerGroup.threshold,
            active = topology.sequencerGroup.active,
            observers = topology.sequencerGroup.passive,
          )
          .valueOr(err =>
            sys.error(s"creating SequencerSynchronizerState should not have failed: $err")
          )
      )

    val partyDataTx = partyToParticipantTxs()

    val synchronizerGovernanceTxs = List(
      mkAdd(
        SynchronizerParametersState(
          synchronizerId,
          synchronizerParametersChangeTx(timestampForSynchronizerParameters),
        )
      )
    )
    val transactions = (participantTxs ++
      synchronizerMembers ++
      mediatorOnboarding ++
      Seq(sequencerOnboarding) ++
      partyDataTx ++
      synchronizerGovernanceTxs)
      .map(ValidatedTopologyTransaction(_, rejectionReason = None))

    val updateF = store.update(
      SequencedTime(CantonTimestamp.Epoch.immediatePredecessor),
      EffectiveTime(CantonTimestamp.Epoch.immediatePredecessor),
      removeMapping = Map.empty,
      removeTxs = Set.empty,
      additions = transactions,
    )(TraceContext.empty)
    Await.result(
      updateF,
      1.seconds,
    ) // The in-memory topology store should complete the state update immediately

    new StoreBasedTopologySnapshot(
      timestampOfSnapshot,
      store,
      packageDependencyResolver,
      loggerFactory,
    )
  }

  private def synchronizerParametersChangeTx(ts: CantonTimestamp): DynamicSynchronizerParameters =
    dynamicSynchronizerParameters.collect { case dp if dp.isValidAt(ts) => dp.parameter } match {
      case dp :: Nil => dp
      case Nil =>
        DynamicSynchronizerParameters.initialValues(
          NonNegativeFiniteDuration.Zero,
          BaseTest.testedProtocolVersion,
        )
      case _ =>
        throw new IllegalStateException(s"Multiple synchronizer parameters are valid at $ts")
    }

  private val signedTxProtocolRepresentative =
    SignedTopologyTransaction.protocolVersionRepresentativeFor(defaultProtocolVersion)

  private def mkAdd(
      mapping: TopologyMapping,
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  ): SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping] =
    SignedTopologyTransaction.create(
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial,
        mapping,
        defaultProtocolVersion,
      ),
      Signature.noSignatures,
      isProposal,
    )(signedTxProtocolRepresentative)

  private def genKeyCollection(
      owner: Member
  ): Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] = {
    implicit val traceContext: TraceContext = TraceContext.empty

    val withFreshKeys = topology.freshKeys.get()

    val keyPurposes = topology.keyPurposes
    val keyName = owner.toProtoPrimitive

    val authName = s"$keyName-${SigningKeyUsage.SequencerAuthentication}"
    val sigName = s"$keyName-${SigningKeyUsage.Protocol}"

    def getOrGenerateSigningKey(
        keyName: KeyName,
        usage: NonEmpty[Set[SigningKeyUsage]],
    ) =
      crypto.cryptoPublicStore
        .findSigningKeyIdByName(keyName)
        .value
        .futureValueUS
        .getOrElse(
          crypto
            .generateSigningKey(name = Some(keyName), usage = usage)
            .valueOrFail("generate signing key")
            .futureValueUS
        )

    def getOrGenerateEncryptionKey(keyName: KeyName) =
      crypto.cryptoPublicStore
        .findEncryptionKeyIdByName(keyName)
        .value
        .futureValueUS
        .getOrElse(
          crypto
            .generateEncryptionKey(name = Some(keyName))
            .valueOrFail("generate signing key")
            .futureValueUS
        )

    if (withFreshKeys)
      crypto match {
        case crypto: SymbolicCrypto =>
          crypto.setRandomKeysFlag(true)
        case _ =>
      }

    val sigKeys =
      if (keyPurposes.contains(KeyPurpose.Signing)) {
        val authKeyName = KeyName.tryCreate(authName)
        val sigKeyName = KeyName.tryCreate(sigName)

        if (withFreshKeys)
          Seq(
            crypto
              .generateSigningKey(
                name = Some(authKeyName),
                usage = SigningKeyUsage.SequencerAuthenticationOnly,
              )
              .valueOrFail("generate sequencer authentication signing key")
              .futureValueUS,
            crypto
              .generateSigningKey(
                name = Some(sigKeyName),
                usage = SigningKeyUsage.ProtocolOnly,
              )
              .valueOrFail("generate protocol signing key")
              .futureValueUS,
          )
        else
          Seq(
            getOrGenerateSigningKey(
              keyName = authKeyName,
              usage = SigningKeyUsage.SequencerAuthenticationOnly,
            ),
            getOrGenerateSigningKey(
              keyName = sigKeyName,
              usage = SigningKeyUsage.ProtocolOnly,
            ),
          )
      } else Nil

    val encKey =
      if (keyPurposes.contains(KeyPurpose.Encryption)) {
        val encKeyName = KeyName.tryCreate(keyName)

        if (withFreshKeys)
          Seq(
            crypto
              .generateEncryptionKey(name = Some(encKeyName))
              .valueOrFail("generate encryption key")
              .futureValueUS
          )
        else
          Seq(getOrGenerateEncryptionKey(keyName = encKeyName))
      } else Nil

    if (withFreshKeys)
      crypto match {
        case crypto: SymbolicCrypto =>
          crypto.setRandomKeysFlag(false)
        case _ =>
      }

    NonEmpty
      .from(sigKeys ++ encKey)
      .map { keys =>
        mkAdd(OwnerToKeyMapping(owner, keys))
      }
      .toList
  }

  private def partyToParticipantTxs()
      : Iterable[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] =
    topology.topology
      .map { case (lfParty, partyInfo) =>
        val partyId = PartyId.tryFromLfParty(lfParty)
        val participantsForParty = partyInfo.participants.filter(_._1.uid != partyId.uid)
        mkAdd(
          PartyToParticipant.tryCreate(
            partyId,
            threshold = partyInfo.threshold,
            participantsForParty.map { case (id, attributes) =>
              HostingParticipant(id, attributes.permission)
            }.toSeq,
          )
        )
      }

  private def participantsTxs(
      defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission],
      packages: Map[ParticipantId, Seq[VettedPackage]],
  ): Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] = topology
    .allParticipants()
    .toSeq
    .flatMap { participantId =>
      val defaultPermission = defaultPermissionByParticipant
        .getOrElse(
          participantId,
          ParticipantPermission.Submission,
        )
      val attributes = topology.participants.getOrElse(
        participantId,
        ParticipantAttributes(defaultPermission, None),
      )
      val pkgs =
        packages
          .get(participantId)
          .map(packages => mkAdd(VettedPackages.tryCreate(participantId, packages)))
          .toList
      pkgs ++ genKeyCollection(participantId) :+ mkAdd(
        SynchronizerTrustCertificate(
          participantId,
          synchronizerId,
        )
      ) :+ mkAdd(
        ParticipantSynchronizerPermission(
          synchronizerId,
          participantId,
          attributes.permission,
          limits = None,
          loginAfter = None,
        )
      )
    }
}

/** something used often: somebody with keys and ability to created signed transactions */
class TestingOwnerWithKeys(
    val keyOwner: Member,
    loggerFactory: NamedLoggerFactory,
    initEc: ExecutionContext,
    multiHash: Boolean = false,
) extends NoTracing {

  val crypto = TestingIdentityFactory(loggerFactory).crypto
  val syncCryptoClient = TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(keyOwner)

  object SigningKeys {

    implicit val ec: ExecutionContext = initEc

    val key1 =
      genSignKey("key1")
    val key1_unsupportedSpec =
      genSignKey("key1", SigningKeyUsage.NamespaceOnly, Some(SigningKeySpec.EcP384))
    val key2 = genSignKey("key2")
    val key3 = genSignKey("key3")
    val key4 = genSignKey("key4")
    val key5 = genSignKey("key5")
    val key6 = genSignKey("key6")
    val key7 = genSignKey("key7")
    val key8 = genSignKey("key8")
    val key9 = genSignKey("key9")

  }

  object EncryptionKeys {
    val key1 = genEncKey("enc-key1")
    val key2 = genEncKey("enc-key2")
    val key3 = genEncKey("enc-key3")
  }

  object TestingTransactions {
    import SigningKeys.*
    val namespaceKey = key1
    val uid2 = UniqueIdentifier.tryCreate("second", uid.namespace)
    val ts = CantonTimestamp.Epoch
    val ts1 = ts.plusSeconds(1)
    val ns1k1 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(namespaceKey.fingerprint),
        namespaceKey,
        CanSignAllMappings,
      )
    )
    val ns1k2 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(namespaceKey.fingerprint),
        key2,
        CanSignAllButNamespaceDelegations,
      )
    )
    val seq_okm_k2 = mkAddMultiKey(
      OwnerToKeyMapping(sequencerId, NonEmpty(Seq, key2)),
      NonEmpty(Set, namespaceKey, key2),
    )
    val med_okm_k3 = mkAddMultiKey(
      OwnerToKeyMapping(mediatorId, NonEmpty(Seq, key3)),
      NonEmpty(Set, namespaceKey, key3),
    )
    val dtc1m =
      SynchronizerTrustCertificate(
        participant1,
        synchronizerId,
      )

    private val defaultSynchronizerParameters = TestSynchronizerParameters.defaultDynamic

    val dpc1 = mkAdd(
      SynchronizerParametersState(
        SynchronizerId(uid),
        defaultSynchronizerParameters
          .tryUpdate(confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
      ),
      namespaceKey,
    )
    val dpc1Updated = mkAdd(
      SynchronizerParametersState(
        SynchronizerId(uid),
        defaultSynchronizerParameters
          .tryUpdate(
            confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2),
            topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(100),
          ),
      ),
      namespaceKey,
    )

    val dpc2 =
      mkAdd(SynchronizerParametersState(SynchronizerId(uid2), defaultSynchronizerParameters), key2)

    val p1_nsk2 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(participant1.fingerprint),
        key2,
        CanSignAllButNamespaceDelegations,
      )
    )
    val p2_nsk2 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(participant2.fingerprint),
        key2,
        CanSignAllButNamespaceDelegations,
      )
    )

    val p1_dtc = mkAdd(SynchronizerTrustCertificate(participant1, synchronizerId))
    val p2_dtc = mkAdd(SynchronizerTrustCertificate(participant2, synchronizerId))
    val p3_dtc = mkAdd(SynchronizerTrustCertificate(participant3, synchronizerId))
    val p1_otk = mkAddMultiKey(
      OwnerToKeyMapping(participant1, NonEmpty(Seq, EncryptionKeys.key1, SigningKeys.key1)),
      NonEmpty(Set, key1),
    )
    val p2_otk = mkAddMultiKey(
      OwnerToKeyMapping(participant2, NonEmpty(Seq, EncryptionKeys.key2, SigningKeys.key2)),
      NonEmpty(Set, key2),
    )
    val p3_otk = mkAddMultiKey(
      OwnerToKeyMapping(participant3, NonEmpty(Seq, EncryptionKeys.key3, SigningKeys.key3)),
      NonEmpty(Set, key3),
    )

    val p1_pdp_observation = mkAdd(
      ParticipantSynchronizerPermission(
        synchronizerId,
        participant1,
        ParticipantPermission.Observation,
        None,
        None,
      )
    )

    val p2_pdp_confirmation = mkAdd(
      ParticipantSynchronizerPermission(
        synchronizerId,
        participant2,
        ParticipantPermission.Confirmation,
        None,
        None,
      )
    )

    val p1p1 = mkAdd(
      PartyToParticipant.tryCreate(
        PartyId(UniqueIdentifier.tryCreate("one", key1.id)),
        PositiveInt.one,
        Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
      )
    )

  }

  def mkTrans[Op <: TopologyChangeOp, M <: TopologyMapping](
      trans: TopologyTransaction[Op, M],
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[Op, M] = {
    // Randomize which hash gets signed when multiHash is true, to create a mix of single and multi signatures
    val hash = if (multiHash && scala.util.Random.nextBoolean()) {
      Some(
        // In practice the other hash should be an actual transaction hash
        // But it actually doesn't matter what the other hash is as long as the transaction hash is included
        // in the hash set
        (
          NonEmpty.mk(Set, trans.hash, TxHash(TestHash.digest("test_hash"))),
          syncCryptoClient.pureCrypto,
        )
      )
    } else {
      None
    }
    Await
      .result(
        SignedTopologyTransaction
          .signAndCreate(
            trans,
            signingKeys.map(_.fingerprint),
            isProposal,
            syncCryptoClient.crypto.privateCrypto,
            BaseTest.testedProtocolVersion,
            multiHash = hash,
          )
          .value,
        10.seconds,
      )
      .onShutdown(sys.error("aborted due to shutdown"))
      .valueOr(err => sys.error(s"failed to create signed topology transaction: $err"))
  }

  def setSerial(
      trans: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping],
      serial: PositiveInt,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
  )(implicit ec: ExecutionContext) = {
    import trans.transaction as tx
    mkTrans(
      TopologyTransaction(
        tx.operation,
        serial,
        tx.mapping,
        tx.representativeProtocolVersion.representative,
      ),
      signingKeys = signingKeys,
    )
  }

  def mkAdd[M <: TopologyMapping](
      mapping: M,
      signingKey: SigningPublicKey = SigningKeys.key1,
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp.Replace, M] =
    mkAddMultiKey(mapping, NonEmpty(Set, signingKey), serial, isProposal)

  def mkAddMultiKey[M <: TopologyMapping](
      mapping: M,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp.Replace, M] =
    mkTrans(
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial,
        mapping,
        BaseTest.testedProtocolVersion,
      ),
      signingKeys,
      isProposal,
    )

  def mkRemoveTx(
      tx: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]
  )(implicit ec: ExecutionContext): SignedTopologyTransaction[Remove, TopologyMapping] =
    mkRemove[TopologyMapping](
      tx.mapping,
      NonEmpty(Set, SigningKeys.key1),
      tx.serial.increment,
    )

  def mkRemove[M <: TopologyMapping](
      mapping: M,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  )(implicit ec: ExecutionContext): SignedTopologyTransaction[TopologyChangeOp.Remove, M] =
    mkTrans(
      TopologyTransaction(
        TopologyChangeOp.Remove,
        serial,
        mapping,
        BaseTest.testedProtocolVersion,
      ),
      signingKeys,
      isProposal,
    )

  def genSignKey(
      name: String,
      // TODO(#25072): Create keys with a single usage and change the tests accordingly
      usage: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(
        Set,
        SigningKeyUsage.Namespace,
        SigningKeyUsage.Protocol,
      ),
      keySpecO: Option[SigningKeySpec] = None,
  ): SigningPublicKey =
    Await
      .result(
        keySpecO.fold(
          syncCryptoClient.crypto
            .generateSigningKey(usage = usage, name = Some(KeyName.tryCreate(name)))
            .value
        )(keySpec =>
          syncCryptoClient.crypto
            .generateSigningKey(
              keySpec = keySpec,
              usage = usage,
              name = Some(KeyName.tryCreate(name)),
            )
            .value
        ),
        30.seconds,
      )
      .onShutdown(sys.error("aborted due to shutdown"))
      .getOrElse(sys.error("key should be there"))

  def genEncKey(name: String): EncryptionPublicKey =
    Await
      .result(
        syncCryptoClient.crypto
          .generateEncryptionKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .onShutdown(sys.error("aborted due to shutdown"))
      .getOrElse(sys.error("key should be there"))

}

object TestingIdentityFactory {

  def apply(
      loggerFactory: NamedLoggerFactory,
      topology: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] = Map.empty,
  ): TestingIdentityFactory =
    TestingIdentityFactory(
      TestingTopology.from(topology = topology),
      loggerFactory,
      TestSynchronizerParameters.defaultDynamic,
      SymbolicCrypto
        .create(testedReleaseProtocolVersion, DefaultProcessingTimeouts.testing, loggerFactory),
    )

  def apply(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
      dynamicSynchronizerParameters: DynamicSynchronizerParameters,
      crypto: SymbolicCrypto,
  ): TestingIdentityFactory = new TestingIdentityFactory(
    topology,
    crypto,
    loggerFactory,
    dynamicSynchronizerParameters = List(
      SynchronizerParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameter = dynamicSynchronizerParameters,
      )
    ),
  )

  def apply(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
      dynamicSynchronizerParameters: DynamicSynchronizerParameters,
  ): TestingIdentityFactory = TestingIdentityFactory(
    topology,
    loggerFactory,
    dynamicSynchronizerParameters,
    SymbolicCrypto.create(
      testedReleaseProtocolVersion,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ),
  )
}
