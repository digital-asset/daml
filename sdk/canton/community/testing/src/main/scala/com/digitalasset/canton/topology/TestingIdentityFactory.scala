// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.BaseTest.testedReleaseProtocolVersion
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.crypto.{Crypto, *}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DomainParameters,
  DynamicDomainParameters,
  TestDomainParameters,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Add
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{MapsUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.mockito.MockitoSugar.mock

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

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
  * The abstraction creates the following hierarchy of classes to resolve the state for a given [[KeyOwner]]
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
    mediators: Set[MediatorId] = Set(DefaultTestIdentities.mediator),
    participants: Map[ParticipantId, ParticipantAttributes] = Map.empty,
    packages: Seq[VettedPackages] = Seq.empty,
    keyPurposes: Set[KeyPurpose] = KeyPurpose.all,
    domainParameters: List[DomainParameters.WithValidity[DynamicDomainParameters]] = List(
      DomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameter = DefaultTestIdentities.defaultDynamicDomainParameters,
      )
    ),
    encKeyTag: Option[String] = None,
) {

  /** Define for which domains the topology should apply.
    *
    * All domains will have exactly the same topology.
    */
  def withDomains(domains: DomainId*): TestingTopology = this.copy(domains = domains.toSet)

  /** Overwrites the `participants` parameter while setting attributes to Submission / Ordinary.
    */
  def withSimpleParticipants(
      participants: ParticipantId*
  ): TestingTopology =
    this.copy(participants =
      participants
        .map(_ -> ParticipantAttributes(ParticipantPermission.Submission, TrustLevel.Ordinary))
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

  def withPackages(packages: Seq[VettedPackages]): TestingTopology =
    this.copy(packages = packages)

  /** This adds a tag to the id of the encryption key during key generation [[genKeyCollection]].
    * Therefore, we can potentially enforce a different key id for the same encryption key.
    */
  def withEncKeyTag(tag: String): TestingTopology =
    this.copy(encKeyTag = Some(tag))

  def build(
      loggerFactory: NamedLoggerFactory = NamedLoggerFactory("test-area", "crypto")
  ): TestingIdentityFactory =
    new TestingIdentityFactory(this, loggerFactory, domainParameters)
}

class TestingIdentityFactory(
    topology: TestingTopology,
    override protected val loggerFactory: NamedLoggerFactory,
    dynamicDomainParameters: List[DomainParameters.WithValidity[DynamicDomainParameters]],
) extends TestingIdentityFactoryBase
    with NamedLogging {

  private val defaultProtocolVersion = BaseTest.testedProtocolVersion

  override protected def domains: Set[DomainId] = topology.domains

  def topologySnapshot(
      domainId: DomainId = DefaultTestIdentities.domainId,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]] =
        StoreBasedDomainTopologyClient.NoPackageDependencies,
      timestampForDomainParameters: CantonTimestamp = CantonTimestamp.Epoch,
  ): TopologySnapshot = {

    val store = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
    )

    // Compute default participant permissions to be the highest granted to an individual party
    val defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission] =
      topology.topology.foldLeft(Map.empty[ParticipantId, ParticipantPermission]) {
        case (acc, (_, permissionByParticipant)) =>
          MapsUtil.extendedMapWith(acc, permissionByParticipant)(ParticipantPermission.higherOf)
      }

    val participantTxs =
      participantsTxs(defaultPermissionByParticipant) ++ topology.packages.map(mkAdd)

    val domainMembers =
      (Seq[KeyOwner](
        DomainTopologyManagerId(domainId),
        SequencerId(domainId),
      ) ++ topology.mediators.toSeq)
        .flatMap(m => genKeyCollection(m))

    val mediatorOnboarding =
      topology.mediators.toSeq.map { mediator =>
        mkAdd(MediatorDomainState(RequestSide.Both, domainId, mediator))
      }

    val partyDataTx = partyToParticipantTxs()

    val domainGovernanceTxs = List(
      mkReplace(
        DomainParametersChange(domainId, domainParametersChangeTx(timestampForDomainParameters))
      )
    )

    val updateF = store.updateState(
      SequencedTime(CantonTimestamp.Epoch.immediatePredecessor),
      EffectiveTime(CantonTimestamp.Epoch.immediatePredecessor),
      deactivate = Seq(),
      positive =
        participantTxs ++ domainMembers ++ mediatorOnboarding ++ partyDataTx ++ domainGovernanceTxs,
    )(TraceContext.empty)
    Await.result(
      updateF,
      1.seconds,
    ) // The in-memory topology store should complete the state update immediately

    new StoreBasedTopologySnapshot(
      CantonTimestamp.Epoch,
      store,
      Map(),
      useStateTxs = true,
      packageDependencies,
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

  private def mkReplace(
      mapping: DomainGovernanceMapping
  ): SignedTopologyTransaction[TopologyChangeOp.Replace] =
    SignedTopologyTransaction(
      DomainGovernanceTransaction(
        DomainGovernanceElement(mapping),
        defaultProtocolVersion,
      ),
      mock[SigningPublicKey],
      Signature.noSignature,
      signedTxProtocolRepresentative,
    )

  private def mkAdd(
      mapping: TopologyStateUpdateMapping
  ): SignedTopologyTransaction[TopologyChangeOp.Add] =
    SignedTopologyTransaction(
      TopologyStateUpdate(
        TopologyChangeOp.Add,
        TopologyStateUpdateElement(TopologyElementId.generate(), mapping),
        defaultProtocolVersion,
      ),
      mock[SigningPublicKey],
      Signature.noSignature,
      signedTxProtocolRepresentative,
    )

  private def genKeyCollection(
      owner: KeyOwner
  ): Seq[SignedTopologyTransaction[TopologyChangeOp.Add]] = {
    val keyPurposes = topology.keyPurposes

    val sigKey =
      if (keyPurposes.contains(KeyPurpose.Signing))
        Seq(
          SymbolicCrypto.signingPublicKey(
            s"sigK-${TestingIdentityFactory.keyFingerprintForOwner(owner).unwrap}"
          )
        )
      else Seq()

    val encKey =
      if (keyPurposes.contains(KeyPurpose.Encryption))
        Seq(
          SymbolicCrypto.encryptionPublicKey(
            s"encK${OptionUtil.noneAsEmptyString(topology.encKeyTag)}-${TestingIdentityFactory.keyFingerprintForOwner(owner).unwrap}"
          )
        )
      else Seq()

    (Seq.empty[PublicKey] ++ sigKey ++ encKey).map { key =>
      mkAdd(OwnerToKeyMapping(owner, key))
    }
  }

  private def partyToParticipantTxs(): Iterable[SignedTopologyTransaction[Add]] = topology.topology
    .flatMap { case (lfParty, participants) =>
      val partyId = PartyId.tryFromLfParty(lfParty)
      participants.iterator.filter(_._1.uid != partyId.uid).map {
        case (participantId, permission) =>
          mkAdd(
            PartyToParticipant(
              RequestSide.Both,
              partyId,
              participantId,
              permission,
            )
          )
      }
    }

  private def participantsTxs(
      defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission]
  ): Seq[SignedTopologyTransaction[Add]] = topology
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
        ParticipantAttributes(defaultPermission, TrustLevel.Ordinary),
      )
      genKeyCollection(participantId) :+ mkAdd(
        ParticipantState(
          RequestSide.Both,
          domainId,
          participantId,
          attributes.permission,
          attributes.trustLevel,
        )
      )
    }

  def newSigningPublicKey(owner: KeyOwner): SigningPublicKey =
    SymbolicCrypto.signingPublicKey(TestingIdentityFactory.keyFingerprintForOwner(owner))

}

trait TestingTopologyTransactionFactory extends NoTracing {

  def crypto: Crypto
  def defaultSigningKey: SigningPublicKey

  def mkTrans[Op <: TopologyChangeOp](
      trans: TopologyTransaction[Op],
      signingKey: SigningPublicKey = defaultSigningKey,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[Op] =
    Await
      .result(
        SignedTopologyTransaction
          .create(
            trans,
            signingKey,
            crypto.pureCrypto,
            crypto.privateCrypto,
            BaseTest.testedProtocolVersion,
          )
          .value,
        10.seconds,
      )
      .getOrElse(sys.error("failed to create signed topology transaction"))

  def mkAdd(mapping: TopologyStateUpdateMapping, signingKey: SigningPublicKey = defaultSigningKey)(
      implicit ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp.Add] =
    mkTrans(TopologyStateUpdate.createAdd(mapping, BaseTest.testedProtocolVersion), signingKey)

  def mkDmGov(mapping: DomainGovernanceMapping, signingKey: SigningPublicKey = defaultSigningKey)(
      implicit ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp.Replace] =
    mkTrans(DomainGovernanceTransaction(mapping, BaseTest.testedProtocolVersion), signingKey)

  def revert(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp] =
    transaction.transaction match {
      case topologyStateUpdate: TopologyStateUpdate[_] =>
        mkTrans(topologyStateUpdate.reverse, transaction.key)
      case DomainGovernanceTransaction(_) =>
        throw new IllegalArgumentException(s"can't revert a domain gov tx $transaction")
    }

  def genSignKey(name: String): SigningPublicKey =
    Await
      .result(
        crypto
          .generateSigningKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .getOrElse(sys.error("key should be there"))

  def genEncKey(name: String): EncryptionPublicKey =
    Await
      .result(
        crypto
          .generateEncryptionKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .getOrElse(sys.error("key should be there"))

}

/** something used often: somebody with keys and ability to created signed transactions */
class TestingOwnerWithKeys(
    val keyOwner: KeyOwner,
    loggerFactory: NamedLoggerFactory,
    initEc: ExecutionContext,
) extends TestingTopologyTransactionFactory {

  val cryptoApi = TestingIdentityFactory(loggerFactory).forOwnerAndDomain(keyOwner)

  override def crypto: Crypto = cryptoApi.crypto
  override def defaultSigningKey: SigningPublicKey = SigningKeys.key1

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
  }

  object TestingTransactions {
    import SigningKeys.*
    val namespaceKey = key1
    val uid2 = uid.copy(id = Identifier.tryCreate("second"))
    val ts = CantonTimestamp.Epoch
    val ts1 = ts.plusSeconds(1)
    val ns1k1 = mkAdd(
      NamespaceDelegation(
        Namespace(namespaceKey.fingerprint),
        namespaceKey,
        isRootDelegation = true,
      )
    )

    val ns1k2 = mkAdd(
      NamespaceDelegation(Namespace(namespaceKey.fingerprint), key2, isRootDelegation = false)
    )
    val id1k1 = mkAdd(IdentifierDelegation(uid, key1))
    val id2k2 = mkAdd(IdentifierDelegation(uid2, key2))
    val okm1 = mkAdd(OwnerToKeyMapping(domainManager, namespaceKey))
    val rokm1 = revert(okm1)
    val okm2 = mkAdd(OwnerToKeyMapping(sequencerId, key2))
    val ps1m =
      ParticipantState(
        RequestSide.Both,
        domainId,
        participant1,
        ParticipantPermission.Submission,
        TrustLevel.Ordinary,
      )
    val ps1 = mkAdd(ps1m)
    val rps1 = revert(ps1)
    val ps2 = mkAdd(ps1m.copy(permission = ParticipantPermission.Observation))
    val ps3 = mkAdd(
      ParticipantState(
        RequestSide.Both,
        domainId,
        participant2,
        ParticipantPermission.Confirmation,
        TrustLevel.Ordinary,
      )
    )

    val p2p1 = mkAdd(
      PartyToParticipant(
        RequestSide.Both,
        PartyId(UniqueIdentifier(Identifier.tryCreate("one"), Namespace(key1.id))),
        participant1,
        ParticipantPermission.Submission,
      )
    )
    val p2p2 = mkAdd(
      PartyToParticipant(
        RequestSide.Both,
        PartyId(UniqueIdentifier(Identifier.tryCreate("two"), Namespace(key1.id))),
        participant1,
        ParticipantPermission.Submission,
      )
    )

    private val defaultDomainParameters = TestDomainParameters.defaultDynamic

    val dpc1 = mkDmGov(
      DomainParametersChange(
        DomainId(uid),
        defaultDomainParameters
          .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
      ),
      namespaceKey,
    )
    val dpc1Updated = mkDmGov(
      DomainParametersChange(
        DomainId(uid),
        defaultDomainParameters
          .tryUpdate(
            participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2),
            topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(100),
          ),
      ),
      namespaceKey,
    )

    val dpc2 =
      mkDmGov(DomainParametersChange(DomainId(uid2), defaultDomainParameters), key2)
  }

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
    )

  def apply(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
      dynamicDomainParameters: DynamicDomainParameters,
  ) = new TestingIdentityFactory(
    topology,
    loggerFactory,
    dynamicDomainParameters = List(
      DomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameter = dynamicDomainParameters,
      )
    ),
  )

  def pureCrypto(): CryptoPureApi = new SymbolicPureCrypto

  def domainClientForOwner(
      owner: KeyOwner,
      domainId: DomainId,
      store: TopologyStore[DomainStore],
      clock: Clock,
      protocolVersion: ProtocolVersion,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      cryptoO: Option[Crypto] = None,
      packageDependencies: Option[PackageId => EitherT[Future, PackageId, Set[PackageId]]] = None,
      useStateTxs: Boolean = true,
      initKeys: Map[KeyOwner, Seq[SigningPublicKey]] = Map(),
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): (DomainTopologyClientWithInit, DomainSyncCryptoClient) = {
    val crypto = cryptoO.getOrElse(newCrypto(loggerFactory)(owner))
    val topologyClient = new StoreBasedDomainTopologyClient(
      clock,
      domainId,
      protocolVersion,
      store,
      initKeys,
      packageDependencies.getOrElse(StoreBasedDomainTopologyClient.NoPackageDependencies),
      DefaultProcessingTimeouts.testing,
      futureSupervisor,
      loggerFactory,
      useStateTxs,
    )
    val cryptoClient = new DomainSyncCryptoClient(
      owner,
      domainId,
      topologyClient,
      crypto = crypto,
      cacheConfigs = CachingConfigs.testing,
      timeouts = DefaultProcessingTimeouts.testing,
      futureSupervisor,
      loggerFactory,
    )
    val initF = store
      .maxTimestamp()
      .map {
        case Some((sequenced, effective)) =>
          topologyClient.updateHead(effective, effective.toApproximate, false)

        case None =>
      }
      .map(_ => (topologyClient, cryptoClient))
    Await.result(initF, 30.seconds)
  }

  private def keyFingerprintForOwner(owner: KeyOwner): Fingerprint =
    // We are converting an Identity (limit of 185 characters) to a Fingerprint (limit of 68 characters) - this would be
    // problematic if this function wasn't only used for testing
    Fingerprint.tryCreate(owner.uid.id.toLengthLimitedString.unwrap)

  def newCrypto(loggerFactory: NamedLoggerFactory)(
      owner: KeyOwner,
      signingFingerprints: Seq[Fingerprint] = Seq(),
      fingerprintSuffixes: Seq[String] = Seq(),
  ): Crypto = {
    val signingFingerprintsOrOwner =
      if (signingFingerprints.isEmpty)
        Seq(keyFingerprintForOwner(owner))
      else
        signingFingerprints

    val fingerprintSuffixesOrOwner =
      if (fingerprintSuffixes.isEmpty)
        Seq(keyFingerprintForOwner(owner).unwrap)
      else
        fingerprintSuffixes

    SymbolicCrypto.tryCreate(
      signingFingerprintsOrOwner,
      fingerprintSuffixesOrOwner,
      testedReleaseProtocolVersion,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
  }

}
