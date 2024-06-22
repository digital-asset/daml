// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.{
  defaultStaticDomainParameters,
  testedReleaseProtocolVersion,
}
import com.digitalasset.canton.concurrent.{
  DirectExecutionContext,
  FutureSupervisor,
  HasFutureSupervision,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DomainParameters,
  DynamicDomainParameters,
  TestDomainParameters,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolverUS,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil}
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPartyId}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

/** Utility functions to setup identity & crypto apis for testing purposes
  *
  * You are trying to figure out how to setup identity topologies and crypto apis to drive your unit tests?
  * Then YOU FOUND IT! The present file contains everything that you should need.
  *
  * First, let's re-call that we are abstracting the identity and crypto aspects from the transaction protocol.
  *
  * Therefore, all the key crypto operations hide behind the so-called crypto-api which splits into the
  * pure part [[CryptoPureApi]] and the more complicated part, the [[SyncCryptoApi]] such that from the transaction
  * protocol perspective, we can conveniently use methods like [[SyncCryptoApi.sign]] or [[SyncCryptoApi.encryptFor]]
  *
  * The abstraction creates the following hierarchy of classes to resolve the state for a given [[Member]]
  * on a per (domainId, timestamp)
  *
  * SyncCryptoApiProvider - root object that makes the synchronisation topology state known to a node accessible
  *   .forDomain          - method to get the specific view on a per domain basis
  * = DomainSyncCryptoApi
  *   .snapshot(timestamp) | recentState - method to get the view for a specific time
  * = DomainSnapshotSyncCryptoApi (extends SyncCryptoApi)
  *
  * All these object carry the necessary objects ([[CryptoPureApi]], [[TopologySnapshot]], [[KeyVaultApi]])
  * as arguments with them.
  *
  * Now, in order to conveniently create a static topology for testing, we provide a
  * <ul>
  *   <li>[[TestingTopology]] which allows us to define a certain static topology</li>
  *   <li>[[TestingIdentityFactory]] which consumes a static topology and delivers all necessary components and
  *       objects that a unit test might need.</li>
  *   <li>[[DefaultTestIdentities]] which provides a predefined set of identities that can be used for unit tests.</li>
  * </ul>
  *
  * Common usage patterns are:
  * <ul>
  *   <li>Get a [[DomainSyncCryptoClient]] with an empty topology: `TestingIdentityFactory().forOwnerAndDomain(participant1)`</li>
  *   <li>To get a [[DomainSnapshotSyncCryptoApi]]: same as above, just add `.recentState`.</li>
  *   <li>Define a specific topology and get the [[SyncCryptoApiProvider]]: `TestingTopology().withTopology(Map(party1 -> participant1)).build()`.</li>
  * </ul>
  *
  * @param domains Set of domains for which the topology is valid
  * @param topology Static association of parties to participants in the most complete way it can be defined in this testing class.
  * @param participants participants for which keys should be added.
  *                     A participant mentioned in `topology` will be included automatically in the topology state,
  *                     so such a participant does not need to be declared again.
  *                     If a participant occurs both in `topology` and `participants`, the attributes of `participants` have higher precedence.
  * @param keyPurposes The purposes of the keys that will be generated.
  */
final case class TestingTopology(
    domains: Set[DomainId] = Set(DefaultTestIdentities.domainId),
    topology: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] = Map.empty,
    mediatorGroups: Set[MediatorGroup] = Set(
      MediatorGroup(
        NonNegativeInt.zero,
        NonEmpty.mk(Seq, DefaultTestIdentities.mediatorId),
        Seq(),
        PositiveInt.one,
      )
    ),
    sequencerGroup: SequencerGroup = SequencerGroup(
      active = NonEmpty.mk(Seq, DefaultTestIdentities.sequencerId),
      passive = Seq.empty,
      threshold = PositiveInt.one,
    ),
    participants: Map[ParticipantId, ParticipantAttributes] = Map.empty,
    packages: Map[ParticipantId, Seq[LfPackageId]] = Map.empty,
    keyPurposes: Set[KeyPurpose] = KeyPurpose.All,
    domainParameters: List[DomainParameters.WithValidity[DynamicDomainParameters]] = List(
      DomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameter = DefaultTestIdentities.defaultDynamicDomainParameters,
      )
    ),
    freshKeys: Boolean = false,
) {
  def mediators: Seq[MediatorId] = mediatorGroups.toSeq.flatMap(_.all)

  /** Define for which domains the topology should apply.
    *
    * All domains will have exactly the same topology.
    */
  def withDomains(domains: DomainId*): TestingTopology = this.copy(domains = domains.toSet)

  def withDynamicDomainParameters(
      dynamicDomainParameters: DynamicDomainParameters,
      validFrom: CantonTimestamp = CantonTimestamp.Epoch,
  ) = {
    copy(
      domainParameters = List(
        DomainParameters.WithValidity(
          validFrom = validFrom,
          validUntil = None,
          parameter = dynamicDomainParameters,
        )
      )
    )
  }

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

  def allParticipants(): Set[ParticipantId] = {
    (topology.values
      .flatMap(x => x.keys) ++ participants.keys).toSet
  }

  def withKeyPurposes(keyPurposes: Set[KeyPurpose]): TestingTopology =
    this.copy(keyPurposes = keyPurposes)

  def withFreshKeys(freshKeys: Boolean): TestingTopology =
    this.copy(freshKeys = freshKeys)

  /** Define the topology as a simple map of party to participant */
  def withTopology(
      parties: Map[LfPartyId, ParticipantId],
      permission: ParticipantPermission = ParticipantPermission.Submission,
  ): TestingTopology = {
    val tmp: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] = parties.toSeq
      .map { case (party, participant) =>
        (party, (participant, permission))
      }
      .groupBy(_._1)
      .fmap(res => res.map(_._2).toMap)
    this.copy(topology = tmp)
  }

  /** Define the topology as a map of participant to map of parties */
  def withReversedTopology(
      parties: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]]
  ): TestingTopology = {
    val converted = parties
      .flatMap { case (participantId, partyToPermission) =>
        partyToPermission.toSeq.map { case (party, permission) =>
          (party, participantId, permission)
        }
      }
      .groupBy(_._1)
      .fmap(_.map { case (_, pid, permission) =>
        (pid, permission)
      }.toMap)
    copy(topology = converted)
  }

  def withPackages(packages: Map[ParticipantId, Seq[LfPackageId]]): TestingTopology =
    this.copy(packages = packages)

  def build(
      loggerFactory: NamedLoggerFactory = NamedLoggerFactory("test-area", "crypto")
  ): TestingIdentityFactory = {
    build(
      SymbolicCrypto.create(
        testedReleaseProtocolVersion,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      ),
      loggerFactory,
    )
  }

  def build(
      crypto: SymbolicCrypto,
      loggerFactory: NamedLoggerFactory,
  ): TestingIdentityFactory =
    new TestingIdentityFactory(this, crypto, loggerFactory, domainParameters)
}

class TestingIdentityFactory(
    topology: TestingTopology,
    crypto: SymbolicCrypto,
    override protected val loggerFactory: NamedLoggerFactory,
    dynamicDomainParameters: List[DomainParameters.WithValidity[DynamicDomainParameters]],
) extends NamedLogging {
  protected implicit val directExecutionContext: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  def forOwner(
      owner: Member,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
      currentSnapshotApproximationTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ): SyncCryptoApiProvider = {
    new SyncCryptoApiProvider(
      owner,
      ips(availableUpToInclusive, currentSnapshotApproximationTimestamp),
      crypto,
      CachingConfigs.testing,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )
  }

  def forOwnerAndDomain(
      owner: Member,
      domain: DomainId = DefaultTestIdentities.domainId,
      availableUpToInclusive: CantonTimestamp = CantonTimestamp.MaxValue,
      currentSnapshotApproximationTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ): DomainSyncCryptoClient =
    forOwner(owner, availableUpToInclusive, currentSnapshotApproximationTimestamp).tryForDomain(
      domain,
      defaultStaticDomainParameters,
    )

  private def ips(
      upToInclusive: CantonTimestamp,
      currentSnapshotApproximationTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ): IdentityProvidingServiceClient = {
    val ips = new IdentityProvidingServiceClient()
    domains.foreach(dId =>
      ips.add(new DomainTopologyClient() with HasFutureSupervision with NamedLogging {

        override protected def loggerFactory: NamedLoggerFactory =
          TestingIdentityFactory.this.loggerFactory

        override protected def futureSupervisor: FutureSupervisor = FutureSupervisor.Noop

        override protected val executionContext: ExecutionContext =
          TestingIdentityFactory.this.directExecutionContext

        override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(
            implicit traceContext: TraceContext
        ): FutureUnlessShutdown[Boolean] = ???

        override def domainId: DomainId = dId

        override def trySnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): TopologySnapshot = {
          require(timestamp <= upToInclusive, "Topology information not yet available")
          topologySnapshot(domainId, timestampForDomainParameters = timestamp)
        }

        override def currentSnapshotApproximation(implicit
            traceContext: TraceContext
        ): TopologySnapshot =
          topologySnapshot(
            domainId,
            timestampForDomainParameters = currentSnapshotApproximationTimestamp,
            timestampOfSnapshot = currentSnapshotApproximationTimestamp,
          )

        override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
          timestamp <= upToInclusive

        override def awaitTimestamp(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(
            implicit traceContext: TraceContext
        ): Option[Future[Unit]] = Option.when(timestamp > upToInclusive) {
          ErrorUtil.internalErrorAsync(
            new IllegalArgumentException(
              s"Attempt to obtain a topology snapshot at $timestamp that will never be available"
            )
          )
        }

        override def awaitTimestampUS(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(
            implicit traceContext: TraceContext
        ): Option[FutureUnlessShutdown[Unit]] =
          awaitTimestamp(timestamp, waitForEffectiveTime).map(FutureUnlessShutdown.outcomeF)

        override def approximateTimestamp: CantonTimestamp =
          currentSnapshotApproximation(TraceContext.empty).timestamp

        override def snapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[TopologySnapshot] = awaitSnapshot(timestamp)

        override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[TopologySnapshot] =
          Future.fromTry(Try(trySnapshot(timestamp)))

        override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] =
          FutureUnlessShutdown.fromTry(Try(trySnapshot(timestamp)))

        override def close(): Unit = ()

        override def topologyKnownUntilTimestamp: CantonTimestamp = upToInclusive

        override def snapshotUS(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] = awaitSnapshotUS(timestamp)
      })
    )
    ips
  }

  private val defaultProtocolVersion = BaseTest.testedProtocolVersion

  private def domains: Set[DomainId] = topology.domains

  def topologySnapshot(
      domainId: DomainId = DefaultTestIdentities.domainId,
      packageDependencyResolver: PackageDependencyResolverUS =
        StoreBasedDomainTopologyClient.NoPackageDependencies,
      timestampForDomainParameters: CantonTimestamp = CantonTimestamp.Epoch,
      timestampOfSnapshot: CantonTimestamp = CantonTimestamp.Epoch,
  ): TopologySnapshot = {

    val store = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      DefaultProcessingTimeouts.testing,
    )

    // Compute default participant permissions to be the highest granted to an individual party
    val defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission] =
      topology.topology.foldLeft(Map.empty[ParticipantId, ParticipantPermission]) {
        case (acc, (_, permissionByParticipant)) =>
          MapsUtil.extendedMapWith(acc, permissionByParticipant)(ParticipantPermission.higherOf)
      }

    val participantTxs = participantsTxs(defaultPermissionByParticipant, topology.packages)

    val domainMembers =
      (topology.sequencerGroup.active.forgetNE ++ topology.sequencerGroup.passive ++ topology.mediators)
        .flatMap(m => genKeyCollection(m))

    val mediatorOnboarding = topology.mediatorGroups.map(group =>
      mkAdd(
        MediatorDomainState
          .create(
            domainId,
            group = group.index,
            threshold = group.threshold,
            active = group.active,
            observers = group.passive,
          )
          .getOrElse(sys.error("creating MediatorDomainState should not have failed"))
      )
    )

    val sequencerOnboarding =
      mkAdd(
        SequencerDomainState
          .create(
            domainId,
            threshold = topology.sequencerGroup.threshold,
            active = topology.sequencerGroup.active.forgetNE,
            observers = topology.sequencerGroup.passive,
          )
          .valueOr(err => sys.error(s"creating SequencerDomainState should not have failed: $err"))
      )

    val partyDataTx = partyToParticipantTxs()

    val domainGovernanceTxs = List(
      mkAdd(
        DomainParametersState(domainId, domainParametersChangeTx(timestampForDomainParameters))
      )
    )
    val transactions = (participantTxs ++
      domainMembers ++
      mediatorOnboarding ++
      Seq(sequencerOnboarding) ++
      partyDataTx ++
      domainGovernanceTxs)
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

  private def domainParametersChangeTx(ts: CantonTimestamp): DynamicDomainParameters =
    dynamicDomainParameters.collect { case dp if dp.isValidAt(ts) => dp.parameter } match {
      case dp :: Nil => dp
      case Nil =>
        DynamicDomainParameters.initialValues(
          NonNegativeFiniteDuration.Zero,
          BaseTest.testedProtocolVersion,
        )
      case _ => throw new IllegalStateException(s"Multiple domain parameters are valid at $ts")
    }

  private val signedTxProtocolRepresentative =
    SignedTopologyTransaction.protocolVersionRepresentativeFor(defaultProtocolVersion)

  private def mkAdd(
      mapping: TopologyMapping,
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  ): SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping] =
    SignedTopologyTransaction(
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
    val keyPurposes = topology.keyPurposes
    val keyName = owner.toProtoPrimitive

    val sigKey =
      if (keyPurposes.contains(KeyPurpose.Signing)) {
        if (topology.freshKeys) {
          crypto.setRandomKeysFlag(true)
          val key = Seq(crypto.generateSymbolicSigningKey(Some(keyName)))
          crypto.setRandomKeysFlag(false)
          key
        } else
          Seq(crypto.getOrGenerateSymbolicSigningKey(keyName))
      } else Seq()

    val encKey =
      if (keyPurposes.contains(KeyPurpose.Encryption)) {
        if (topology.freshKeys) {
          crypto.setRandomKeysFlag(true)
          val key = Seq(crypto.generateSymbolicEncryptionKey(Some(keyName)))
          crypto.setRandomKeysFlag(false)
          key
        } else
          Seq(crypto.getOrGenerateSymbolicEncryptionKey(keyName))
      } else Seq()

    NonEmpty
      .from(sigKey ++ encKey)
      .map { keys =>
        mkAdd(OwnerToKeyMapping(owner, None, keys))
      }
      .toList
  }

  private def partyToParticipantTxs()
      : Iterable[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] =
    topology.topology
      .map { case (lfParty, participants) =>
        val partyId = PartyId.tryFromLfParty(lfParty)
        val participantsForParty = participants.iterator.filter(_._1.uid != partyId.uid)
        mkAdd(
          PartyToParticipant
            .tryCreate(
              partyId,
              None,
              threshold = PositiveInt.one,
              participantsForParty.map { case (id, permission) =>
                HostingParticipant(id, permission)
              }.toSeq,
              groupAddressing = false,
            )
        )
      }

  private def participantsTxs(
      defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission],
      packages: Map[ParticipantId, Seq[LfPackageId]],
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
          .map(packages => mkAdd(VettedPackages(participantId, None, packages)))
          .toSeq
      pkgs ++ genKeyCollection(participantId) :+ mkAdd(
        DomainTrustCertificate(
          participantId,
          domainId,
          transferOnlyToGivenTargetDomains = false,
          targetDomains = Seq.empty,
        )
      ) :+ mkAdd(
        ParticipantDomainPermission(
          domainId,
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
) extends NoTracing {

  val cryptoApi = TestingIdentityFactory(loggerFactory).forOwnerAndDomain(keyOwner)

  object SigningKeys {

    implicit val ec: ExecutionContext = initEc

    val key1 = genSignKey("key1")
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
    private implicit val ec: ExecutionContext = initEc
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
        isRootDelegation = true,
      )
    )
    val ns1k2 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(namespaceKey.fingerprint),
        key2,
        isRootDelegation = false,
      )
    )
    val id1k1 = mkAdd(IdentifierDelegation(uid, key1))
    val id2k2 = mkAdd(IdentifierDelegation(uid2, key2))
    val seq_okm_k2 = mkAddMultiKey(
      OwnerToKeyMapping(sequencerId, None, NonEmpty(Seq, key2)),
      NonEmpty(Set, namespaceKey, key2),
    )
    val med_okm_k3 = mkAddMultiKey(
      OwnerToKeyMapping(mediatorId, None, NonEmpty(Seq, key3)),
      NonEmpty(Set, namespaceKey, key3),
    )
    val dtc1m =
      DomainTrustCertificate(
        participant1,
        domainId,
        transferOnlyToGivenTargetDomains = false,
        targetDomains = Seq.empty,
      )

    private val defaultDomainParameters = TestDomainParameters.defaultDynamic

    val dpc1 = mkAdd(
      DomainParametersState(
        DomainId(uid),
        defaultDomainParameters
          .tryUpdate(confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
      ),
      namespaceKey,
    )
    val dpc1Updated = mkAdd(
      DomainParametersState(
        DomainId(uid),
        defaultDomainParameters
          .tryUpdate(
            confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2),
            topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(100),
          ),
      ),
      namespaceKey,
    )

    val dpc2 =
      mkAdd(DomainParametersState(DomainId(uid2), defaultDomainParameters), key2)

    val p1_nsk2 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(participant1.fingerprint),
        key2,
        isRootDelegation = false,
      )
    )
    val p2_nsk2 = mkAdd(
      NamespaceDelegation.tryCreate(
        Namespace(participant2.fingerprint),
        key2,
        isRootDelegation = false,
      )
    )

    val p1_dtc = mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty))
    val p2_dtc = mkAdd(DomainTrustCertificate(participant2, domainId, false, Seq.empty))
    val p3_dtc = mkAdd(DomainTrustCertificate(participant3, domainId, false, Seq.empty))
    val p1_otk = mkAddMultiKey(
      OwnerToKeyMapping(participant1, None, NonEmpty(Seq, EncryptionKeys.key1, SigningKeys.key1)),
      NonEmpty(Set, key1),
    )
    val p2_otk = mkAddMultiKey(
      OwnerToKeyMapping(participant2, None, NonEmpty(Seq, EncryptionKeys.key2, SigningKeys.key2)),
      NonEmpty(Set, key2),
    )
    val p3_otk = mkAddMultiKey(
      OwnerToKeyMapping(participant3, None, NonEmpty(Seq, EncryptionKeys.key3, SigningKeys.key3)),
      NonEmpty(Set, key3),
    )

    val p1_pdp_observation = mkAdd(
      ParticipantDomainPermission(
        domainId,
        participant1,
        ParticipantPermission.Observation,
        None,
        None,
      )
    )

    val p2_pdp_confirmation = mkAdd(
      ParticipantDomainPermission(
        domainId,
        participant2,
        ParticipantPermission.Confirmation,
        None,
        None,
      )
    )

    val p1p1 = mkAdd(
      PartyToParticipant.tryCreate(
        PartyId(UniqueIdentifier.tryCreate("one", key1.id)),
        None,
        PositiveInt.one,
        Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
        groupAddressing = false,
      )
    )

  }

  def mkTrans[Op <: TopologyChangeOp, M <: TopologyMapping](
      trans: TopologyTransaction[Op, M],
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[Op, M] =
    Await
      .result(
        SignedTopologyTransaction
          .create(
            trans,
            signingKeys.map(_.id),
            isProposal,
            cryptoApi.crypto.privateCrypto,
            BaseTest.testedProtocolVersion,
          )
          .value,
        10.seconds,
      )
      .onShutdown(sys.error("aborted due to shutdown"))
      .getOrElse(sys.error("failed to create signed topology transaction"))

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
  private def genSignKey(name: String): SigningPublicKey =
    Await
      .result(
        cryptoApi.crypto
          .generateSigningKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .onShutdown(sys.error("aborted due to shutdown"))
      .getOrElse(sys.error("key should be there"))

  def genEncKey(name: String): EncryptionPublicKey =
    Await
      .result(
        cryptoApi.crypto
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
      TestingTopology(topology = topology),
      loggerFactory,
      TestDomainParameters.defaultDynamic,
      SymbolicCrypto
        .create(testedReleaseProtocolVersion, DefaultProcessingTimeouts.testing, loggerFactory),
    )

  def apply(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
      dynamicDomainParameters: DynamicDomainParameters,
      crypto: SymbolicCrypto,
  ): TestingIdentityFactory = new TestingIdentityFactory(
    topology,
    crypto,
    loggerFactory,
    dynamicDomainParameters = List(
      DomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameter = dynamicDomainParameters,
      )
    ),
  )

  def apply(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
      dynamicDomainParameters: DynamicDomainParameters,
  ): TestingIdentityFactory = TestingIdentityFactory(
    topology,
    loggerFactory,
    dynamicDomainParameters,
    SymbolicCrypto.create(
      testedReleaseProtocolVersion,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ),
  )

}
