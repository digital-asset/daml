// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.testedReleaseProtocolVersion
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.data.CantonTimestamp
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
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{TopologyStoreId, ValidatedTopologyTransactionX}
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX.Remove
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{MapsUtil, OptionUtil}
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPartyId}

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
  *   <li>[[TestingTopologyX]] which allows us to define a certain static topology</li>
  *   <li>[[TestingIdentityFactory]] which consumes a static topology and delivers all necessary components and
  *       objects that a unit test might need.</li>
  *   <li>[[DefaultTestIdentities]] which provides a predefined set of identities that can be used for unit tests.</li>
  * </ul>
  *
  * Common usage patterns are:
  * <ul>
  *   <li>Get a [[DomainSyncCryptoClient]] with an empty topology: `TestingIdentityFactory().forOwnerAndDomain(participant1)`</li>
  *   <li>To get a [[DomainSnapshotSyncCryptoApi]]: same as above, just add `.recentState`.</li>
  *   <li>Define a specific topology and get the [[SyncCryptoApiProvider]]: `TestingTopologyX().withTopology(Map(party1 -> participant1)).build()`.</li>
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
final case class TestingTopologyX(
    domains: Set[DomainId] = Set(DefaultTestIdentities.domainId),
    topology: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] = Map.empty,
    mediatorGroups: Set[MediatorGroup] = Set(
      MediatorGroup(
        NonNegativeInt.zero,
        Seq(DefaultTestIdentities.mediatorIdX),
        Seq(),
        PositiveInt.one,
      )
    ),
    sequencerGroup: SequencerGroup = SequencerGroup(
      active = Seq(DefaultTestIdentities.sequencerIdX),
      passive = Seq.empty,
      threshold = PositiveInt.one,
    ),
    participants: Map[ParticipantId, ParticipantAttributes] = Map.empty,
    packages: Map[ParticipantId, Seq[LfPackageId]] = Map.empty,
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
  def mediators: Seq[MediatorId] = mediatorGroups.toSeq.flatMap(_.all)

  /** Define for which domains the topology should apply.
    *
    * All domains will have exactly the same topology.
    */
  def withDomains(domains: DomainId*): TestingTopologyX = this.copy(domains = domains.toSet)

  /** Overwrites the `sequencerGroup` field.
    */
  def withSequencerGroup(
      sequencerGroup: SequencerGroup
  ): TestingTopologyX =
    this.copy(sequencerGroup = sequencerGroup)

  /** Overwrites the `participants` parameter while setting attributes to Submission / Ordinary.
    */
  def withSimpleParticipants(
      participants: ParticipantId*
  ): TestingTopologyX =
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
  ): TestingTopologyX =
    this.copy(participants = participants.toMap)

  def allParticipants(): Set[ParticipantId] = {
    (topology.values
      .flatMap(x => x.keys) ++ participants.keys).toSet
  }

  def withKeyPurposes(keyPurposes: Set[KeyPurpose]): TestingTopologyX =
    this.copy(keyPurposes = keyPurposes)

  /** This adds a tag to the id of the encryption key during key generation [[genKeyCollection]].
    * Therefore, we can potentially enforce a different key id for the same encryption key.
    */
  def withEncKeyTag(tag: String): TestingTopologyX =
    this.copy(encKeyTag = Some(tag))

  /** Define the topology as a simple map of party to participant */
  def withTopology(
      parties: Map[LfPartyId, ParticipantId],
      permission: ParticipantPermission = ParticipantPermission.Submission,
  ): TestingTopologyX = {
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
  ): TestingTopologyX = {
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

  def withPackages(packages: Map[ParticipantId, Seq[LfPackageId]]): TestingTopologyX =
    this.copy(packages = packages)

  def build(
      loggerFactory: NamedLoggerFactory = NamedLoggerFactory("test-area", "crypto")
  ): TestingIdentityFactoryX =
    new TestingIdentityFactoryX(this, loggerFactory, domainParameters)
}

class TestingIdentityFactoryX(
    topology: TestingTopologyX,
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

    val store = new InMemoryTopologyStoreX(
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
      (topology.sequencerGroup.active ++ topology.sequencerGroup.passive ++ topology.mediators.toSeq)
        .flatMap(m => genKeyCollection(m))

    val mediatorOnboarding = topology.mediatorGroups.map(group =>
      mkAdd(
        MediatorDomainStateX
          .create(
            domainId,
            group = group.index,
            threshold = group.threshold,
            active = group.active,
            observers = group.passive,
          )
          .getOrElse(sys.error("creating MediatorDomainStateX should not have failed"))
      )
    )

    val sequencerOnboarding =
      mkAdd(
        SequencerDomainStateX
          .create(
            domainId,
            threshold = topology.sequencerGroup.threshold,
            active = topology.sequencerGroup.active,
            observers = topology.sequencerGroup.passive,
          )
          .getOrElse(sys.error("creating SequencerDomainStateX should not have failed"))
      )

    val partyDataTx = partyToParticipantTxs()

    val domainGovernanceTxs = List(
      mkAdd(
        DomainParametersStateX(domainId, domainParametersChangeTx(timestampForDomainParameters))
      )
    )
    val transactions = (participantTxs ++
      domainMembers ++
      mediatorOnboarding ++
      Seq(sequencerOnboarding) ++
      partyDataTx ++
      domainGovernanceTxs)
      .map(ValidatedTopologyTransactionX(_, rejectionReason = None))

    val updateF = store.update(
      SequencedTime(CantonTimestamp.Epoch.immediatePredecessor),
      EffectiveTime(CantonTimestamp.Epoch.immediatePredecessor),
      removeMapping = Set.empty,
      removeTxs = Set.empty,
      additions = transactions,
    )(TraceContext.empty)
    Await.result(
      updateF,
      1.seconds,
    ) // The in-memory topology store should complete the state update immediately

    new StoreBasedTopologySnapshotX(
      CantonTimestamp.Epoch,
      store,
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
    SignedTopologyTransactionX.protocolVersionRepresentativeFor(defaultProtocolVersion)

  private def mkAdd(
      mapping: TopologyMappingX,
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  ): SignedTopologyTransactionX[TopologyChangeOpX.Replace, TopologyMappingX] =
    SignedTopologyTransactionX(
      TopologyTransactionX(
        TopologyChangeOpX.Replace,
        serial,
        mapping,
        defaultProtocolVersion,
      ),
      Signature.noSignatures,
      isProposal,
    )(signedTxProtocolRepresentative)

  private def genKeyCollection(
      owner: Member
  ): Seq[SignedTopologyTransactionX[TopologyChangeOpX.Replace, TopologyMappingX]] = {
    val keyPurposes = topology.keyPurposes

    val sigKey =
      if (keyPurposes.contains(KeyPurpose.Signing))
        Seq(SymbolicCrypto.signingPublicKey(s"sigK-${keyFingerprintForOwner(owner).unwrap}"))
      else Seq()

    val encKey =
      if (keyPurposes.contains(KeyPurpose.Encryption))
        Seq(
          SymbolicCrypto.encryptionPublicKey(
            s"encK${OptionUtil.noneAsEmptyString(topology.encKeyTag)}-${keyFingerprintForOwner(owner).unwrap}"
          )
        )
      else Seq()

    NonEmpty
      .from(sigKey ++ encKey)
      .map { keys =>
        mkAdd(OwnerToKeyMappingX(owner, None, keys))
      }
      .toList
  }

  private def partyToParticipantTxs()
      : Iterable[SignedTopologyTransactionX[TopologyChangeOpX.Replace, TopologyMappingX]] =
    topology.topology
      .map { case (lfParty, participants) =>
        val partyId = PartyId.tryFromLfParty(lfParty)
        val participantsForParty = participants.iterator.filter(_._1.uid != partyId.uid)
        mkAdd(
          PartyToParticipantX(
            partyId,
            None,
            threshold = PositiveInt.one,
            participantsForParty.map { case (id, permission) =>
              HostingParticipant(id, permission.tryToX)
            }.toSeq,
            groupAddressing = false,
          )
        )
      }

  private def participantsTxs(
      defaultPermissionByParticipant: Map[ParticipantId, ParticipantPermission],
      packages: Map[ParticipantId, Seq[LfPackageId]],
  ): Seq[SignedTopologyTransactionX[TopologyChangeOpX.Replace, TopologyMappingX]] = topology
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
          .map(packages => mkAdd(VettedPackagesX(participantId, None, packages)))
          .toSeq
      pkgs ++ genKeyCollection(participantId) :+ mkAdd(
        DomainTrustCertificateX(
          participantId,
          domainId,
          transferOnlyToGivenTargetDomains = false,
          targetDomains = Seq.empty,
        )
      ) :+ mkAdd(
        ParticipantDomainPermissionX(
          domainId,
          participantId,
          attributes.permission.tryToX,
          limits = None,
          loginAfter = None,
        )
      )
    }

  private def keyFingerprintForOwner(owner: Member): Fingerprint =
    // We are converting an Identity (limit of 185 characters) to a Fingerprint (limit of 68 characters) - this would be
    // problematic if this function wasn't only used for testing
    Fingerprint.tryCreate(owner.uid.id.toLengthLimitedString.unwrap)

  def newCrypto(
      owner: Member,
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

  def newSigningPublicKey(owner: Member): SigningPublicKey = {
    SymbolicCrypto.signingPublicKey(keyFingerprintForOwner(owner))
  }

}

/** something used often: somebody with keys and ability to created signed transactions */
class TestingOwnerWithKeysX(
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
    val uid2 = uid.copy(id = Identifier.tryCreate("second"))
    val ts = CantonTimestamp.Epoch
    val ts1 = ts.plusSeconds(1)
    val ns1k1 = mkAdd(
      NamespaceDelegationX.tryCreate(
        Namespace(namespaceKey.fingerprint),
        namespaceKey,
        isRootDelegation = true,
      )
    )
    val ns1k2 = mkAdd(
      NamespaceDelegationX.tryCreate(
        Namespace(namespaceKey.fingerprint),
        key2,
        isRootDelegation = false,
      )
    )
    val id1k1 = mkAdd(IdentifierDelegationX(uid, key1))
    val id2k2 = mkAdd(IdentifierDelegationX(uid2, key2))
    val okm1 = mkAdd(OwnerToKeyMappingX(domainManager, None, NonEmpty(Seq, namespaceKey)))
    val seq_okm_k2 = mkAdd(OwnerToKeyMappingX(sequencerIdX, None, NonEmpty(Seq, key2)))
    val med_okm_k3 = mkAdd(OwnerToKeyMappingX(mediatorIdX, None, NonEmpty(Seq, key3)))
    val dtc1m =
      DomainTrustCertificateX(
        participant1,
        domainId,
        transferOnlyToGivenTargetDomains = false,
        targetDomains = Seq.empty,
      )

    private val defaultDomainParameters = TestDomainParameters.defaultDynamic

    val dpc1 = mkAdd(
      DomainParametersStateX(
        DomainId(uid),
        defaultDomainParameters
          .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
      ),
      namespaceKey,
    )
    val dpc1Updated = mkAdd(
      DomainParametersStateX(
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
      mkAdd(DomainParametersStateX(DomainId(uid2), defaultDomainParameters), key2)

    val p1_nsk2 = mkAdd(
      NamespaceDelegationX.tryCreate(
        Namespace(participant1.uid.namespace.fingerprint),
        key2,
        isRootDelegation = false,
      )
    )
    val p2_nsk2 = mkAdd(
      NamespaceDelegationX.tryCreate(
        Namespace(participant2.uid.namespace.fingerprint),
        key2,
        isRootDelegation = false,
      )
    )

    val p1_dtc = mkAdd(DomainTrustCertificateX(participant1, domainId, false, Seq.empty))
    val p2_dtc = mkAdd(DomainTrustCertificateX(participant2, domainId, false, Seq.empty))
    val p3_dtc = mkAdd(DomainTrustCertificateX(participant3, domainId, false, Seq.empty))
    val p1_otk = mkAdd(
      OwnerToKeyMappingX(participant1, None, NonEmpty(Seq, EncryptionKeys.key1, SigningKeys.key1))
    )
    val p2_otk = mkAdd(
      OwnerToKeyMappingX(participant2, None, NonEmpty(Seq, EncryptionKeys.key2, SigningKeys.key2))
    )
    val p3_otk = mkAdd(
      OwnerToKeyMappingX(participant3, None, NonEmpty(Seq, EncryptionKeys.key3, SigningKeys.key3))
    )

    val p1_pdp_observation = mkAdd(
      ParticipantDomainPermissionX(
        domainId,
        participant1,
        ParticipantPermissionX.Observation,
        None,
        None,
      )
    )

    val p2_pdp_confirmation = mkAdd(
      ParticipantDomainPermissionX(
        domainId,
        participant2,
        ParticipantPermissionX.Confirmation,
        None,
        None,
      )
    )

  }

  def mkTrans[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      trans: TopologyTransactionX[Op, M],
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransactionX[Op, M] =
    Await
      .result(
        SignedTopologyTransactionX
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
      .getOrElse(sys.error("failed to create signed topology transaction"))

  def setSerial(
      trans: SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX],
      serial: PositiveInt,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
  )(implicit ec: ExecutionContext) = {
    import trans.transaction as tx
    mkTrans(
      TopologyTransactionX(
        tx.op,
        serial,
        tx.mapping,
        tx.representativeProtocolVersion.representative,
      ),
      signingKeys = signingKeys,
    )
  }

  def mkAdd[M <: TopologyMappingX](
      mapping: M,
      signingKey: SigningPublicKey = SigningKeys.key1,
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransactionX[TopologyChangeOpX.Replace, M] =
    mkAddMultiKey(mapping, NonEmpty(Set, signingKey), serial, isProposal)

  def mkAddMultiKey[M <: TopologyMappingX](
      mapping: M,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransactionX[TopologyChangeOpX.Replace, M] =
    mkTrans(
      TopologyTransactionX(
        TopologyChangeOpX.Replace,
        serial,
        mapping,
        BaseTest.testedProtocolVersion,
      ),
      signingKeys,
      isProposal,
    )

  def mkRemoveTx(
      tx: SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]
  )(implicit ec: ExecutionContext): SignedTopologyTransactionX[Remove, TopologyMappingX] =
    mkRemove[TopologyMappingX](
      tx.mapping,
      NonEmpty(Set, SigningKeys.key1),
      tx.transaction.serial.increment,
    )

  def mkRemove[M <: TopologyMappingX](
      mapping: M,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = NonEmpty(Set, SigningKeys.key1),
      serial: PositiveInt = PositiveInt.one,
      isProposal: Boolean = false,
  )(implicit ec: ExecutionContext): SignedTopologyTransactionX[TopologyChangeOpX.Remove, M] =
    mkTrans(
      TopologyTransactionX(
        TopologyChangeOpX.Remove,
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
      .getOrElse(sys.error("key should be there"))

  def genEncKey(name: String): EncryptionPublicKey =
    Await
      .result(
        cryptoApi.crypto
          .generateEncryptionKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .getOrElse(sys.error("key should be there"))

}

object TestingIdentityFactoryX {

  def apply(
      loggerFactory: NamedLoggerFactory,
      topology: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]] = Map.empty,
  ): TestingIdentityFactoryX =
    TestingIdentityFactoryX(
      TestingTopologyX(topology = topology),
      loggerFactory,
      TestDomainParameters.defaultDynamic,
    )

  def apply(
      topology: TestingTopologyX,
      loggerFactory: NamedLoggerFactory,
      dynamicDomainParameters: DynamicDomainParameters,
  ) = new TestingIdentityFactoryX(
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

  def newCrypto(loggerFactory: NamedLoggerFactory)(
      owner: Member,
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

  private def keyFingerprintForOwner(owner: Member): Fingerprint =
    // We are converting an Identity (limit of 185 characters) to a Fingerprint (limit of 68 characters) - this would be
    // problematic if this function wasn't only used for testing
    Fingerprint.tryCreate(owner.uid.id.toLengthLimitedString.unwrap)

}
