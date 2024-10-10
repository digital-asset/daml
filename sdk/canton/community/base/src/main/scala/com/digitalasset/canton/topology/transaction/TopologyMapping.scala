// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.Monoid
import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  UnrecognizedEnum,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30.Enums
import com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping
import com.digitalasset.canton.protocol.{DynamicDomainParameters, DynamicSequencingParameters, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.RequiredAuth.*
import com.digitalasset.canton.topology.transaction.TopologyMapping.{
  Code,
  MappingHash,
  RequiredAuth,
}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.ProtoVersion
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.SetParameter

import scala.annotation.nowarn
import scala.math.Ordering.Implicits.*
import scala.reflect.ClassTag

sealed trait TopologyMapping extends Product with Serializable with PrettyPrinting {

  require(maybeUid.forall(_.namespace == namespace), "namespace is inconsistent")

  override protected def pretty: Pretty[this.type] = adHocPrettyInstance

  /** Returns the code used to store & index this mapping */
  def code: Code

  /** The "primary" namespace authorizing the topology mapping.
    * Used for filtering query results.
    */
  def namespace: Namespace

  /** The "primary" identity authorizing the topology mapping, optional as some mappings (namespace delegations and
    * decentralized namespace definitions) only have a namespace
    * Used for filtering query results.
    */
  def maybeUid: Option[UniqueIdentifier]

  /** Returns authorization information
    *
    * Each topology transaction must be authorized directly or indirectly by
    * all necessary controllers of the given namespace.
    *
    * @param previous the previously validly authorized state (some state changes only need subsets of the authorizers)
    */
  def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth

  def restrictedToDomain: Option[DomainId]

  def toProtoV30: v30.TopologyMapping

  def uniqueKey: MappingHash

  final def select[TargetMapping <: TopologyMapping](implicit
      M: ClassTag[TargetMapping]
  ): Option[TargetMapping] = M.unapply(this)

}

object TopologyMapping {
  private[transaction] def participantIdFromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[ParticipantId] = {
    /*
    We changed some of the topology protobufs from `string participant_id` (member id, with PAR prefix)
    to string participant_uid` (uid of the participant, without PAR prefix).
    This allows backward compatible parsing.

    The fallback can be removed if/when all the topology transactions in the global synchronizer are
    recreated from scratch.
     */

    val participantIdOld = ParticipantId.fromProtoPrimitive(proto = proto, fieldName = fieldName)
    participantIdOld.orElse(
      UniqueIdentifier.fromProtoPrimitive(uid = proto, fieldName = fieldName).map(ParticipantId(_))
    )
  }

  private[transaction] def buildUniqueKey(code: TopologyMapping.Code)(
      addUniqueKeyToBuilder: HashBuilder => HashBuilder
  ): MappingHash =
    MappingHash(
      addUniqueKeyToBuilder(
        Hash.build(HashPurpose.TopologyMappingUniqueKey, HashAlgorithm.Sha256)
      ).add(code.dbInt)
        .finish()
    )

  final case class MappingHash(hash: Hash) extends AnyVal

  sealed abstract class Code private (val dbInt: Int, val code: String)
      extends Product
      with Serializable
  object Code {

    case object NamespaceDelegation extends Code(1, "nsd")
    case object IdentifierDelegation extends Code(2, "idd")
    case object DecentralizedNamespaceDefinition extends Code(3, "dnd")

    case object OwnerToKeyMapping extends Code(4, "otk")

    case object DomainTrustCertificate extends Code(5, "dtc")
    case object ParticipantDomainPermission extends Code(6, "pdp")
    case object PartyHostingLimits extends Code(7, "phl")
    case object VettedPackages extends Code(8, "vtp")

    case object PartyToParticipant extends Code(9, "ptp")

    // reserved Code(10), was AuthorityOf

    case object DomainParametersState extends Code(11, "dop")
    case object MediatorDomainState extends Code(12, "mds")
    case object SequencerDomainState extends Code(13, "sds")
    case object OffboardParticipant extends Code(14, "ofp")

    case object PurgeTopologyTransaction extends Code(15, "ptt")
    // Don't reuse 16, It was the TrafficControlState code mapping
    case object SequencingDynamicParametersState extends Code(17, "sep")
    case object PartyToKeyMapping extends Code(18, "ptk")

    lazy val all: Seq[Code] = Seq(
      NamespaceDelegation,
      IdentifierDelegation,
      DecentralizedNamespaceDefinition,
      OwnerToKeyMapping,
      DomainTrustCertificate,
      ParticipantDomainPermission,
      PartyHostingLimits,
      VettedPackages,
      PartyToParticipant,
      DomainParametersState,
      MediatorDomainState,
      SequencerDomainState,
      OffboardParticipant,
      PurgeTopologyTransaction,
      PartyToKeyMapping,
    )

    def fromString(code: String): ParsingResult[Code] =
      all
        .find(_.code == code)
        .toRight(UnrecognizedEnum("TopologyMapping.Code", code, all.map(_.code)))

    implicit val setParameterTopologyMappingCode: SetParameter[Code] =
      (v, pp) => pp.setInt(v.dbInt)

  }

  // Small wrapper to not have to work with (Set[Namespace], Set[Namespace], Set[Uid])
  final case class ReferencedAuthorizations(
      namespacesWithRoot: Set[Namespace] = Set.empty,
      namespaces: Set[Namespace] = Set.empty,
      uids: Set[UniqueIdentifier] = Set.empty,
      extraKeys: Set[Fingerprint] = Set.empty,
  ) extends PrettyPrinting {
    def isEmpty: Boolean =
      namespacesWithRoot.isEmpty && namespaces.isEmpty && uids.isEmpty && extraKeys.isEmpty

    override protected def pretty: Pretty[ReferencedAuthorizations.this.type] = prettyOfClass(
      paramIfNonEmpty("namespacesWithRoot", _.namespacesWithRoot),
      paramIfNonEmpty("namespaces", _.namespaces),
      paramIfNonEmpty("uids", _.uids),
      paramIfNonEmpty("extraKeys", _.extraKeys),
    )
  }

  object ReferencedAuthorizations {

    val empty: ReferencedAuthorizations = ReferencedAuthorizations()

    implicit val monoid: Monoid[ReferencedAuthorizations] =
      new Monoid[ReferencedAuthorizations] {
        override def empty: ReferencedAuthorizations = ReferencedAuthorizations.empty

        override def combine(
            x: ReferencedAuthorizations,
            y: ReferencedAuthorizations,
        ): ReferencedAuthorizations =
          ReferencedAuthorizations(
            namespacesWithRoot = x.namespacesWithRoot ++ y.namespacesWithRoot,
            namespaces = x.namespaces ++ y.namespaces,
            uids = x.uids ++ y.uids,
            extraKeys = x.extraKeys ++ y.extraKeys,
          )
      }
  }

  sealed trait RequiredAuth extends PrettyPrinting {
    def requireRootDelegation: Boolean = false
    def satisfiedByActualAuthorizers(
        provided: ReferencedAuthorizations
    ): Either[ReferencedAuthorizations, Unit]

    final def or(next: RequiredAuth): RequiredAuth =
      RequiredAuth.Or(this, next)

    /** Authorizations referenced by this instance.
      * Note that the result is not equivalent to this instance, as an "or" gets translated to an "and".
      * Instead, the result indicates which authorization keys need to be evaluated in order to check
      * if this RequiredAuth is met.
      */
    def referenced: ReferencedAuthorizations
  }

  object RequiredAuth {

    private[transaction] case object EmptyAuthorization extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] = Either.unit

      override def referenced: ReferencedAuthorizations = ReferencedAuthorizations()

      override protected def pretty: Pretty[EmptyAuthorization.this.type] = adHocPrettyInstance
    }

    final case class RequiredNamespaces(
        namespaces: Set[Namespace],
        override val requireRootDelegation: Boolean = false,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] = {
        val filter = if (requireRootDelegation) provided.namespacesWithRoot else provided.namespaces
        val missing = namespaces.filter(ns => !filter(ns))
        Either.cond(
          missing.isEmpty,
          (),
          ReferencedAuthorizations(
            namespacesWithRoot = if (requireRootDelegation) missing else Set.empty,
            namespaces = if (requireRootDelegation) Set.empty else missing,
          ),
        )
      }

      override def referenced: ReferencedAuthorizations = ReferencedAuthorizations(
        namespacesWithRoot = if (requireRootDelegation) namespaces else Set.empty,
        namespaces = if (requireRootDelegation) Set.empty else namespaces,
      )

      override protected def pretty: Pretty[RequiredNamespaces.this.type] = prettyOfClass(
        unnamedParam(_.namespaces),
        paramIfTrue("requireRootDelegation", _.requireRootDelegation),
      )
    }

    final case class RequiredUids(
        uids: Set[UniqueIdentifier],
        extraKeys: Set[Fingerprint] = Set.empty,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] = {
        val missingUids =
          uids.filter(uid => !provided.uids(uid) && !provided.namespaces(uid.namespace))
        val missingExtraKeys = extraKeys -- provided.extraKeys
        val missingAuth =
          ReferencedAuthorizations(uids = missingUids, extraKeys = missingExtraKeys)
        Either.cond(
          missingAuth.isEmpty,
          (),
          missingAuth,
        )
      }

      override def referenced: ReferencedAuthorizations = ReferencedAuthorizations(
        uids = uids,
        extraKeys = extraKeys,
      )

      override protected def pretty: Pretty[RequiredUids.this.type] = prettyOfClass(
        paramIfNonEmpty("uids", _.uids),
        paramIfNonEmpty("extraKeys", _.extraKeys),
      )
    }

    private[transaction] final case class Or(
        first: RequiredAuth,
        second: RequiredAuth,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] =
        first
          .satisfiedByActualAuthorizers(provided)
          .orElse(second.satisfiedByActualAuthorizers(provided))

      override def referenced: ReferencedAuthorizations =
        ReferencedAuthorizations.monoid.combine(first.referenced, second.referenced)

      override protected def pretty: Pretty[Or.this.type] =
        prettyOfClass(unnamedParam(_.first), unnamedParam(_.second))
    }
  }

  def fromProtoV30(proto: v30.TopologyMapping): ParsingResult[TopologyMapping] =
    proto.mapping match {
      case Mapping.Empty =>
        Left(ProtoDeserializationError.TransactionDeserialization("No mapping set"))
      case Mapping.NamespaceDelegation(value) => NamespaceDelegation.fromProtoV30(value)
      case Mapping.IdentifierDelegation(value) => IdentifierDelegation.fromProtoV30(value)
      case Mapping.DecentralizedNamespaceDefinition(value) =>
        DecentralizedNamespaceDefinition.fromProtoV30(value)
      case Mapping.OwnerToKeyMapping(value) => OwnerToKeyMapping.fromProtoV30(value)
      case Mapping.PartyToKeyMapping(value) => PartyToKeyMapping.fromProtoV30(value)
      case Mapping.DomainTrustCertificate(value) => DomainTrustCertificate.fromProtoV30(value)
      case Mapping.PartyHostingLimits(value) => PartyHostingLimits.fromProtoV30(value)
      case Mapping.ParticipantPermission(value) => ParticipantDomainPermission.fromProtoV30(value)
      case Mapping.VettedPackages(value) => VettedPackages.fromProtoV30(value)
      case Mapping.PartyToParticipant(value) => PartyToParticipant.fromProtoV30(value)
      case Mapping.DomainParametersState(value) => DomainParametersState.fromProtoV30(value)
      case Mapping.SequencingDynamicParametersState(value) =>
        DynamicSequencingParametersState.fromProtoV30(value)
      case Mapping.MediatorDomainState(value) => MediatorDomainState.fromProtoV30(value)
      case Mapping.SequencerDomainState(value) => SequencerDomainState.fromProtoV30(value)
      case Mapping.PurgeTopologyTxs(value) => PurgeTopologyTransaction.fromProtoV30(value)
    }

  private[transaction] def addDomainId(
      builder: HashBuilder,
      domainId: Option[DomainId],
  ): HashBuilder =
    builder.add(domainId.map(_.uid.toProtoPrimitive).getOrElse("none"))

}

/** A namespace delegation transaction (intermediate CA)
  *
  * Entrusts a public-key to perform changes on the namespace
  * {(*,I) => p_k}
  *
  * If the delegation is a root delegation, then the target key
  * inherits the right to authorize other NamespaceDelegations.
  */
final case class NamespaceDelegation private (
    namespace: Namespace,
    target: SigningPublicKey,
    isRootDelegation: Boolean,
) extends TopologyMapping {

  def toProto: v30.NamespaceDelegation =
    v30.NamespaceDelegation(
      namespace = namespace.fingerprint.unwrap,
      targetKey = Some(target.toProtoV30),
      isRootDelegation = isRootDelegation,
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.NamespaceDelegation(
        toProto
      )
    )

  override def code: Code = Code.NamespaceDelegation

  override def maybeUid: Option[UniqueIdentifier] = None

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    // All namespace delegation creations require the root delegation privilege.
    RequiredNamespaces(Set(namespace), requireRootDelegation = true)

  override lazy val uniqueKey: MappingHash =
    NamespaceDelegation.uniqueKey(namespace, target.fingerprint)
}

object NamespaceDelegation {

  def uniqueKey(namespace: Namespace, target: Fingerprint): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap).add(target.unwrap))

  def create(
      namespace: Namespace,
      target: SigningPublicKey,
      isRootDelegation: Boolean,
  ): Either[String, NamespaceDelegation] =
    Either.cond(
      isRootDelegation || namespace.fingerprint != target.fingerprint,
      NamespaceDelegation(namespace, target, isRootDelegation),
      s"Root certificate for $namespace needs to be set as isRootDelegation = true",
    )

  @VisibleForTesting
  protected[canton] def tryCreate(
      namespace: Namespace,
      target: SigningPublicKey,
      isRootDelegation: Boolean,
  ): NamespaceDelegation =
    create(namespace, target, isRootDelegation).valueOr(err =>
      throw new IllegalArgumentException((err))
    )

  def code: TopologyMapping.Code = Code.NamespaceDelegation

  /** Returns true if the given transaction is a self-signed root certificate */
  def isRootCertificate(sit: GenericSignedTopologyTransaction): Boolean =
    sit.mapping
      .select[transaction.NamespaceDelegation]
      .exists(ns =>
        sit.signatures.size == 1 &&
          sit.signatures.head1.signedBy == ns.namespace.fingerprint &&
          ns.isRootDelegation &&
          ns.target.fingerprint == ns.namespace.fingerprint
      )

  /** Returns true if the given transaction is a root delegation */
  def isRootDelegation(sit: GenericSignedTopologyTransaction): Boolean =
    isRootCertificate(sit) || (
      sit.operation == TopologyChangeOp.Replace &&
        sit.mapping
          .select[transaction.NamespaceDelegation]
          .exists(ns => ns.isRootDelegation)
    )

  def fromProtoV30(
      value: v30.NamespaceDelegation
  ): ParsingResult[NamespaceDelegation] =
    for {
      namespace <- Fingerprint.fromProtoPrimitive(value.namespace).map(Namespace(_))
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "target_key",
        value.targetKey,
      )
    } yield NamespaceDelegation(namespace, target, value.isRootDelegation)

}

/** Defines a decentralized namespace
  *
  * authorization: whoever controls the domain and all the owners of the active or observing sequencers that
  *   were not already present in the tx with serial = n - 1
  *   exception: a sequencer can leave the consortium unilaterally as long as there are enough members
  *              to reach the threshold
  */
final case class DecentralizedNamespaceDefinition private (
    override val namespace: Namespace,
    threshold: PositiveInt,
    owners: NonEmpty[Set[Namespace]],
) extends TopologyMapping {

  def toProto: v30.DecentralizedNamespaceDefinition =
    v30.DecentralizedNamespaceDefinition(
      decentralizedNamespace = namespace.fingerprint.unwrap,
      threshold = threshold.unwrap,
      owners = owners.toSeq.map(_.toProtoPrimitive),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.DecentralizedNamespaceDefinition(toProto)
    )

  override def code: Code = Code.DecentralizedNamespaceDefinition

  override def maybeUid: Option[UniqueIdentifier] = None

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    previous
      .collect {
        case TopologyTransaction(
              _op,
              _serial,
              DecentralizedNamespaceDefinition(`namespace`, _previousThreshold, previousOwners),
            ) =>
          val added = owners.diff(previousOwners)
          // all added owners and the quorum of existing owners MUST sign
          RequiredNamespaces(added + namespace)
      }
      .getOrElse(RequiredNamespaces(owners.forgetNE))

  override def uniqueKey: MappingHash = DecentralizedNamespaceDefinition.uniqueKey(namespace)
}

object DecentralizedNamespaceDefinition {

  def uniqueKey(namespace: Namespace): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap))

  def code: TopologyMapping.Code = Code.DecentralizedNamespaceDefinition

  def create(
      decentralizedNamespace: Namespace,
      threshold: PositiveInt,
      owners: NonEmpty[Set[Namespace]],
  ): Either[String, DecentralizedNamespaceDefinition] =
    for {
      _ <- Either.cond(
        owners.size >= threshold.value,
        (),
        s"Invalid threshold ($threshold) for $decentralizedNamespace with ${owners.size} owners",
      )
    } yield DecentralizedNamespaceDefinition(decentralizedNamespace, threshold, owners)

  def fromProtoV30(
      value: v30.DecentralizedNamespaceDefinition
  ): ParsingResult[DecentralizedNamespaceDefinition] = {
    val v30.DecentralizedNamespaceDefinition(decentralizedNamespaceP, thresholdP, ownersP) = value
    for {
      decentralizedNamespace <- Fingerprint
        .fromProtoPrimitive(decentralizedNamespaceP)
        .map(Namespace(_))
      threshold <- ProtoConverter.parsePositiveInt("threshold", thresholdP)
      owners <- ownersP.traverse(Fingerprint.fromProtoPrimitive)
      ownersNE <- NonEmpty
        .from(owners.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            field = "owners",
            error = "cannot be empty",
          )
        )
      item <- create(decentralizedNamespace, threshold, ownersNE.map(Namespace(_)))
        .leftMap(ProtoDeserializationError.OtherError.apply)
    } yield item
  }

  def computeNamespace(
      owners: Set[Namespace]
  ): Namespace = {
    val builder = Hash.build(HashPurpose.DecentralizedNamespaceNamespace, HashAlgorithm.Sha256)
    owners.toSeq
      .sorted(Namespace.namespaceOrder.toOrdering)
      .foreach(ns => builder.add(ns.fingerprint.unwrap))
    Namespace(Fingerprint.tryCreate(builder.finish().toLengthLimitedHexString))
  }
}

/** An identifier delegation
  *
  * entrusts a public-key to do any change with respect to the identifier
  * {(X,I) => p_k}
  */
final case class IdentifierDelegation(identifier: UniqueIdentifier, target: SigningPublicKey)
    extends TopologyMapping {

  def toProto: v30.IdentifierDelegation =
    v30.IdentifierDelegation(
      uniqueIdentifier = identifier.toProtoPrimitive,
      targetKey = Some(target.toProtoV30),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.IdentifierDelegation(
        toProto
      )
    )

  override def code: Code = Code.IdentifierDelegation

  override def namespace: Namespace = identifier.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(identifier)

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(Set(namespace), requireRootDelegation = false)

  override def uniqueKey: MappingHash =
    IdentifierDelegation.uniqueKey(identifier, target.fingerprint)
}

object IdentifierDelegation {

  def uniqueKey(identifier: UniqueIdentifier, targetKey: Fingerprint): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(identifier.toProtoPrimitive).add(targetKey.unwrap))

  def code: Code = Code.IdentifierDelegation

  def fromProtoV30(
      value: v30.IdentifierDelegation
  ): ParsingResult[IdentifierDelegation] =
    for {
      identifier <- UniqueIdentifier.fromProtoPrimitive(value.uniqueIdentifier, "unique_identifier")
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "target_key",
        value.targetKey,
      )
    } yield IdentifierDelegation(identifier, target)
}

/** A key owner (participant, mediator, sequencer) to key mapping
  *
  * In Canton, we need to know keys for all participating entities. The entities are
  * all the protocol members (participant, mediator) plus the
  * sequencer (which provides the communication infrastructure for the protocol members).
  */
final case class OwnerToKeyMapping(
    member: Member,
    keys: NonEmpty[Seq[PublicKey]],
) extends TopologyMapping {

  def toProto: v30.OwnerToKeyMapping = v30.OwnerToKeyMapping(
    member = member.toProtoPrimitive,
    publicKeys = keys.map(_.toProtoPublicKeyV30),
  )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.OwnerToKeyMapping(
        toProto
      )
    )

  def code: TopologyMapping.Code = Code.OwnerToKeyMapping

  override def namespace: Namespace = member.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(member.uid)

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = {
    val previouslyRegisteredKeys = previous
      .flatMap(_.selectOp[TopologyChangeOp.Replace])
      .flatMap(_.selectMapping[OwnerToKeyMapping])
      .toList
      .flatMap(_.mapping.keys.map(_.fingerprint).forgetNE)
      .toSet
    val newKeys = keys.filter(_.isSigning).map(_.fingerprint).toSet -- previouslyRegisteredKeys
    RequiredUids(Set(member.uid), extraKeys = newKeys)
  }

  override def uniqueKey: MappingHash = OwnerToKeyMapping.uniqueKey(member)
}

object OwnerToKeyMapping {

  def uniqueKey(member: Member): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(member.uid.toProtoPrimitive))

  def code: TopologyMapping.Code = Code.OwnerToKeyMapping

  def fromProtoV30(
      value: v30.OwnerToKeyMapping
  ): ParsingResult[OwnerToKeyMapping] = {
    val v30.OwnerToKeyMapping(memberP, keysP) = value
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      keys <- keysP.traverse(x =>
        ProtoConverter
          .parseRequired(PublicKey.fromProtoPublicKeyV30, "public_keys", Some(x))
      )
      keysNE <- NonEmpty
        .from(keys)
        .toRight(ProtoDeserializationError.FieldNotSet("public_keys"): ProtoDeserializationError)
    } yield OwnerToKeyMapping(member, keysNE)
  }

}

/** A party to key mapping
  *
  * In Canton, we can delegate the submission authorisation to a participant
  * node, or we can sign the transaction outside of the node with a party
  * key. This mapping is used to map the party to a set of public keys authorized to sign submissions.
  */
final case class PartyToKeyMapping private (
    party: PartyId,
    domain: Option[DomainId],
    threshold: PositiveInt,
    signingKeys: NonEmpty[Seq[SigningPublicKey]],
) extends TopologyMapping {

  def toProto: v30.PartyToKeyMapping = v30.PartyToKeyMapping(
    party = party.toProtoPrimitive,
    domain = domain.map(_.toProtoPrimitive).getOrElse(""),
    threshold = threshold.unwrap,
    signingKeys = signingKeys.map(_.toProtoV30),
  )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyToKeyMapping(
        toProto
      )
    )

  def code: TopologyMapping.Code = Code.PartyToKeyMapping

  override def namespace: Namespace = party.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(party.uid)

  override def restrictedToDomain: Option[DomainId] = domain

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = {
    val previouslyRegisteredKeys = previous
      .flatMap(_.select[TopologyChangeOp.Replace, PartyToKeyMapping])
      .toList
      .flatMap(_.mapping.signingKeys.forgetNE)
      .toSet
    val newKeys = signingKeys.toSet -- previouslyRegisteredKeys
    RequiredUids(Set(party.uid), newKeys.map(_.fingerprint))
  }

  override def uniqueKey: MappingHash = PartyToKeyMapping.uniqueKey(party, domain)
}

object PartyToKeyMapping {

  def create(
      partyId: PartyId,
      domainId: Option[DomainId],
      threshold: PositiveInt,
      signingKeys: NonEmpty[Seq[SigningPublicKey]],
  ): Either[String, PartyToKeyMapping] = {
    val noDuplicateKeys = {
      val duplicateKeys = signingKeys.groupBy(_.fingerprint).values.filter(_.size > 1).toList
      Either.cond(
        duplicateKeys.isEmpty,
        (),
        s"All signing keys must be unique. Duplicate keys: $duplicateKeys",
      )
    }

    val thresholdCanBeMet =
      Either
        .cond(
          threshold.value <= signingKeys.size,
          (),
          s"Party $partyId cannot meet threshold of $threshold signing keys with participants ${signingKeys.size} keys",
        )
        .map(_ => PartyToKeyMapping(partyId, domainId, threshold, signingKeys))

    noDuplicateKeys.flatMap(_ => thresholdCanBeMet)
  }

  def tryCreate(
      partyId: PartyId,
      domainId: Option[DomainId],
      threshold: PositiveInt,
      signingKeys: NonEmpty[Seq[SigningPublicKey]],
  ): PartyToKeyMapping =
    create(partyId, domainId, threshold, signingKeys).valueOr(err =>
      throw new IllegalArgumentException(err)
    )

  def uniqueKey(party: PartyId, domain: Option[DomainId]): MappingHash =
    TopologyMapping.buildUniqueKey(code)(b =>
      TopologyMapping.addDomainId(b.add(party.uid.toProtoPrimitive), domain)
    )

  def code: TopologyMapping.Code = Code.PartyToKeyMapping

  def fromProtoV30(
      value: v30.PartyToKeyMapping
  ): ParsingResult[PartyToKeyMapping] = {
    val v30.PartyToKeyMapping(partyP, domainP, thresholdP, signingKeysP) = value
    for {
      party <- PartyId.fromProtoPrimitive(partyP, "party")
      signingKeysNE <-
        ProtoConverter.parseRequiredNonEmpty(
          SigningPublicKey.fromProtoV30,
          "signing_keys",
          signingKeysP,
        )
      domain <- OptionUtil
        .emptyStringAsNone(domainP)
        .traverse(DomainId.fromProtoPrimitive(_, "domain"))
      threshold <- PositiveInt
        .create(thresholdP)
        .leftMap(InvariantViolation.toProtoDeserializationError("threshold", _))
    } yield PartyToKeyMapping(party, domain, threshold, signingKeysNE)
  }

}

/** Participant domain trust certificate
  */
final case class DomainTrustCertificate(
    participantId: ParticipantId,
    domainId: DomainId,
) extends TopologyMapping {

  def toProto: v30.DomainTrustCertificate =
    v30.DomainTrustCertificate(
      participantUid = participantId.uid.toProtoPrimitive,
      domain = domainId.toProtoPrimitive,
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.DomainTrustCertificate(
        toProto
      )
    )

  override def code: Code = Code.DomainTrustCertificate

  override def namespace: Namespace = participantId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredUids(Set(participantId.uid))

  override def uniqueKey: MappingHash = DomainTrustCertificate.uniqueKey(participantId, domainId)
}

object DomainTrustCertificate {

  def uniqueKey(participantId: ParticipantId, domainId: DomainId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(participantId.toProtoPrimitive).add(domainId.toProtoPrimitive)
    )

  def code: Code = Code.DomainTrustCertificate

  def fromProtoV30(
      value: v30.DomainTrustCertificate
  ): ParsingResult[DomainTrustCertificate] =
    for {
      participantId <- TopologyMapping.participantIdFromProtoPrimitive(
        value.participantUid,
        "participant_uid",
      )
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
    } yield DomainTrustCertificate(
      participantId,
      domainId,
    )
}

/** Permissions of a participant, i.e., things a participant can do on behalf of a party
  *
  * Permissions are hierarchical. A participant who can submit can confirm. A participant who can confirm can observe.
  */
sealed abstract class ParticipantPermission(val canConfirm: Boolean)
    extends Product
    with Serializable {
  def toProtoV30: v30.Enums.ParticipantPermission
}
object ParticipantPermission {
  case object Submission extends ParticipantPermission(canConfirm = true) {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
  }
  case object Confirmation extends ParticipantPermission(canConfirm = true) {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
  }
  case object Observation extends ParticipantPermission(canConfirm = false) {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
  }

  def fromProtoV30(
      value: v30.Enums.ParticipantPermission
  ): ParsingResult[ParticipantPermission] =
    value match {
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED =>
        Left(FieldNotSet(value.name))
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
        Right(Submission)
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
        Right(Confirmation)
      case v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
        Right(Observation)
      case v30.Enums.ParticipantPermission.Unrecognized(x) =>
        Left(UnrecognizedEnum(value.name, x))
    }

  implicit val orderingParticipantPermission: Ordering[ParticipantPermission] = {
    val ParticipantPermissionOrderMap = Seq[ParticipantPermission](
      Observation,
      Confirmation,
      Submission,
    ).zipWithIndex.toMap
    Ordering.by[ParticipantPermission, Int](ParticipantPermissionOrderMap(_))
  }

  def lowerOf(fst: ParticipantPermission, snd: ParticipantPermission): ParticipantPermission =
    fst.min(snd)

  def higherOf(fst: ParticipantPermission, snd: ParticipantPermission): ParticipantPermission =
    fst.max(snd)
}

/** @param confirmationRequestsMaxRate maximum number of mediator confirmation requests sent per participant per second
  */
final case class ParticipantDomainLimits(
    confirmationRequestsMaxRate: NonNegativeInt
) extends PrettyPrinting {

  override protected def pretty: Pretty[ParticipantDomainLimits] =
    prettyOfClass(
      param("confirmation requests max rate", _.confirmationRequestsMaxRate)
    )

  def toProto: v30.ParticipantDomainLimits =
    v30.ParticipantDomainLimits(confirmationRequestsMaxRate.unwrap)
}
object ParticipantDomainLimits {
  def fromProtoV30(value: v30.ParticipantDomainLimits): ParsingResult[ParticipantDomainLimits] =
    for {
      confirmationRequestsMaxRate <- NonNegativeInt
        .create(value.confirmationRequestsMaxRate)
        .leftMap(ProtoDeserializationError.InvariantViolation("confirmation_requests_max_rate", _))
    } yield ParticipantDomainLimits(confirmationRequestsMaxRate)
}
final case class ParticipantDomainPermission(
    domainId: DomainId,
    participantId: ParticipantId,
    permission: ParticipantPermission,
    limits: Option[ParticipantDomainLimits],
    loginAfter: Option[CantonTimestamp],
) extends TopologyMapping {

  def toParticipantAttributes: ParticipantAttributes =
    ParticipantAttributes(permission, loginAfter)

  def toProto: v30.ParticipantDomainPermission =
    v30.ParticipantDomainPermission(
      domain = domainId.toProtoPrimitive,
      participantUid = participantId.uid.toProtoPrimitive,
      permission = permission.toProtoV30,
      limits = limits.map(_.toProto),
      loginAfter = loginAfter.map(_.toProtoPrimitive),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.ParticipantPermission(
        toProto
      )
    )

  override def code: Code = Code.ParticipantDomainPermission

  override def namespace: Namespace = participantId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredUids(Set(domainId.uid))

  override def uniqueKey: MappingHash =
    ParticipantDomainPermission.uniqueKey(domainId, participantId)

  def setDefaultLimitIfNotSet(
      defaultLimits: ParticipantDomainLimits
  ): ParticipantDomainPermission =
    if (limits.nonEmpty)
      this
    else
      ParticipantDomainPermission(
        domainId,
        participantId,
        permission,
        Some(defaultLimits),
        loginAfter,
      )
}

object ParticipantDomainPermission {

  def uniqueKey(domainId: DomainId, participantId: ParticipantId): MappingHash =
    TopologyMapping.buildUniqueKey(
      code
    )(_.add(domainId.toProtoPrimitive).add(participantId.toProtoPrimitive))

  def code: Code = Code.ParticipantDomainPermission

  def default(
      domainId: DomainId,
      participantId: ParticipantId,
  ): ParticipantDomainPermission =
    ParticipantDomainPermission(
      domainId,
      participantId,
      ParticipantPermission.Submission,
      None,
      None,
    )

  def fromProtoV30(
      value: v30.ParticipantDomainPermission
  ): ParsingResult[ParticipantDomainPermission] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      participantId <- TopologyMapping.participantIdFromProtoPrimitive(
        value.participantUid,
        "participant_uid",
      )
      permission <- ParticipantPermission.fromProtoV30(value.permission)
      limits <- value.limits.traverse(ParticipantDomainLimits.fromProtoV30)
      loginAfter <- value.loginAfter.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield ParticipantDomainPermission(
      domainId,
      participantId,
      permission,
      limits,
      loginAfter,
    )
}

// Party hosting limits
final case class PartyHostingLimits(
    domainId: DomainId,
    partyId: PartyId,
) extends TopologyMapping {

  def toProto: v30.PartyHostingLimits =
    v30.PartyHostingLimits(
      domain = domainId.toProtoPrimitive,
      party = partyId.toProtoPrimitive,
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyHostingLimits(
        toProto
      )
    )

  override def code: Code = Code.PartyHostingLimits

  override def namespace: Namespace = partyId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredUids(Set(domainId.uid))

  override def uniqueKey: MappingHash = PartyHostingLimits.uniqueKey(domainId, partyId)
}

object PartyHostingLimits {

  def uniqueKey(domainId: DomainId, partyId: PartyId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(domainId.toProtoPrimitive).add(partyId.toProtoPrimitive)
    )

  def code: Code = Code.PartyHostingLimits

  def fromProtoV30(
      value: v30.PartyHostingLimits
  ): ParsingResult[PartyHostingLimits] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
    } yield PartyHostingLimits(domainId, partyId)
}

/** Represents a package with an optional validity period. No start or end means that the validity
  * of the package is unbounded. The validity period is expected to be compared to the
  * ledger effective time of Daml transactions.
  * @param packageId the hash of the package
  * @param validFrom optional exclusive start of the validity period
  * @param validUntil optional inclusive end of the validity period
  */
final case class VettedPackage(
    packageId: LfPackageId,
    validFrom: Option[CantonTimestamp],
    validUntil: Option[CantonTimestamp],
) extends PrettyPrinting {

  def validAt(ts: CantonTimestamp): Boolean = validFrom.forall(_ < ts) && validUntil.forall(_ >= ts)

  def toProtoV30: v30.VettedPackages.VettedPackage = v30.VettedPackages.VettedPackage(
    packageId,
    validFrom = validFrom.map(_.toProtoTimestamp),
    validUntil = validUntil.map(_.toProtoTimestamp),
  )
  override protected def pretty: Pretty[VettedPackage.this.type] = prettyOfClass(
    param("packageId", _.packageId),
    paramIfDefined("validFrom", _.validFrom),
    paramIfDefined("validUntil", _.validUntil),
    paramIfTrue("unbounded", vp => vp.validFrom.isEmpty && vp.validUntil.isEmpty),
  )
}

object VettedPackage {
  def unbounded(packageIds: Seq[LfPackageId]): Seq[VettedPackage] =
    packageIds.map(VettedPackage(_, None, None))

  def fromProtoV30(
      value: v30.VettedPackages.VettedPackage
  ): ParsingResult[VettedPackage] = for {
    pkgId <- LfPackageId
      .fromString(value.packageId)
      .leftMap(ProtoDeserializationError.ValueConversionError("package_id", _))
    validFrom <- value.validFrom.traverse(CantonTimestamp.fromProtoTimestamp)
    validUntil <- value.validUntil.traverse(CantonTimestamp.fromProtoTimestamp)
  } yield VettedPackage(pkgId, validFrom, validUntil)
}

// Package vetting
final case class VettedPackages private (
    participantId: ParticipantId,
    packages: Seq[VettedPackage],
) extends TopologyMapping {

  def toProto: v30.VettedPackages =
    v30.VettedPackages(
      participantUid = participantId.uid.toProtoPrimitive,
      packageIds = Seq.empty,
      packages = packages.map(_.toProtoV30),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.VettedPackages(
        toProto
      )
    )

  override def code: Code = Code.VettedPackages

  override def namespace: Namespace = participantId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredUids(Set(participantId.uid))

  override def uniqueKey: MappingHash = VettedPackages.uniqueKey(participantId)
}

object VettedPackages {

  def uniqueKey(participantId: ParticipantId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(participantId.toProtoPrimitive))

  def code: Code = Code.VettedPackages

  def create(
      participantId: ParticipantId,
      packages: Seq[VettedPackage],
  ): Either[String, VettedPackages] = {
    val multipleValidityPeriods = packages
      .groupBy(_.packageId)
      .view
      .collect { case (_, pkgs) if pkgs.sizeIs > 1 => pkgs }
      .flatten
      .toList

    val emptyValidity = packages.filter(vp =>
      (vp.validFrom, vp.validUntil).tupled.exists { case (from, until) => from >= until }
    )

    for {
      _ <- Either.cond(
        multipleValidityPeriods.isEmpty,
        (),
        s"a package may only have one validty period: ${multipleValidityPeriods.mkString(", ")}",
      )
      _ <- Either.cond(
        emptyValidity.isEmpty,
        (),
        s"packages with empty validity period: ${emptyValidity.mkString(", ")}",
      )
    } yield {
      VettedPackages(participantId, packages)
    }
  }

  def tryCreate(
      participantId: ParticipantId,
      packages: Seq[VettedPackage],
  ): VettedPackages =
    create(participantId, packages).valueOr(err => throw new IllegalArgumentException(err))

  @nowarn("cat=deprecation")
  def fromProtoV30(
      value: v30.VettedPackages
  ): ParsingResult[VettedPackages] =
    for {
      participantId <- TopologyMapping.participantIdFromProtoPrimitive(
        value.participantUid,
        "participant_uid",
      )
      packageIdsUnbounded <- value.packageIds
        .traverse(
          LfPackageId
            .fromString(_)
            .leftMap(ProtoDeserializationError.ValueConversionError("package_ids", _))
        )
        .map(VettedPackage.unbounded)
      packages <- value.packages.traverse(VettedPackage.fromProtoV30)

      duplicatePackages = packageIdsUnbounded
        .map(_.packageId)
        .intersect(packages.map(_.packageId))
        .toSet
      _ <- Either.cond(
        duplicatePackages.isEmpty,
        (),
        ProtoDeserializationError.InvariantViolation(
          None,
          s"packages $duplicatePackages are listed in both fields 'package_ids' and 'packages' but may only be set in one.",
        ),
      )
    } yield VettedPackages(participantId, packageIdsUnbounded ++ packages)
}

// Party to participant mappings
final case class HostingParticipant(
    participantId: ParticipantId,
    permission: ParticipantPermission,
) {
  def toProto: v30.PartyToParticipant.HostingParticipant =
    v30.PartyToParticipant.HostingParticipant(
      participantUid = participantId.uid.toProtoPrimitive,
      permission = permission.toProtoV30,
    )
}

object HostingParticipant {
  def fromProtoV30(
      value: v30.PartyToParticipant.HostingParticipant
  ): ParsingResult[HostingParticipant] = for {
    participantId <- TopologyMapping.participantIdFromProtoPrimitive(
      value.participantUid,
      "participant_uid",
    )
    permission <- ParticipantPermission.fromProtoV30(value.permission)
  } yield HostingParticipant(participantId, permission)
}

final case class PartyToParticipant private (
    partyId: PartyId,
    threshold: PositiveInt,
    participants: Seq[HostingParticipant],
) extends TopologyMapping {

  def toProto: v30.PartyToParticipant =
    v30.PartyToParticipant(
      party = partyId.toProtoPrimitive,
      threshold = threshold.value,
      participants = participants.map(_.toProto),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyToParticipant(
        toProto
      )
    )

  override def code: Code = Code.PartyToParticipant

  override def namespace: Namespace = partyId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToDomain: Option[DomainId] = None

  def participantIds: Seq[ParticipantId] = participants.map(_.participantId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    previous
      .collect {
        case TopologyTransaction(
              TopologyChangeOp.Replace,
              _,
              PartyToParticipant(_, prevThreshold, prevParticipants),
            ) =>
          val current = this
          val currentParticipantIds = participants.map(_.participantId.uid).toSet
          val prevParticipantIds = prevParticipants.map(_.participantId.uid).toSet
          val removedParticipants = prevParticipantIds -- currentParticipantIds
          val addedParticipants = currentParticipantIds -- prevParticipantIds

          val contentHasChanged = prevThreshold != current.threshold

          // check whether a participant can unilaterally unhost a party
          if (
            // no change in threshold
            !contentHasChanged
            // no participant added
            && addedParticipants.isEmpty
            // only 1 participant removed
            && removedParticipants.sizeCompare(1) == 0
          ) {
            // This scenario can either be authorized by the party or the single participant removed from the mapping
            RequiredUids(Set(partyId.uid)).or(RequiredUids(removedParticipants))
          } else {
            // all other cases requires the party's and the new (possibly) new participants' signature
            RequiredUids(Set(partyId.uid) ++ addedParticipants)
          }
      }
      .getOrElse(
        RequiredUids(Set(partyId.uid) ++ participants.map(_.participantId.uid))
      )

  override def uniqueKey: MappingHash = PartyToParticipant.uniqueKey(partyId)
}

object PartyToParticipant {

  def create(
      partyId: PartyId,
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
  ): Either[String, PartyToParticipant] = {
    val noDuplicatePParticipants = {
      val duplicatePermissions =
        participants.groupBy(_.participantId).values.filter(_.size > 1).toList
      Either.cond(
        duplicatePermissions.isEmpty,
        (),
        s"Participants may only be assigned one permission: $duplicatePermissions",
      )
    }
    val thresholdCanBeMet = {
      val numConfirmingParticipants =
        participants.count(_.permission >= ParticipantPermission.Confirmation)
      Either
        .cond(
          // we allow to not meet the threshold criteria if there are only observing participants.
          // but as soon as there is 1 confirming participant, the threshold must theoretically be satisfiable,
          // otherwise the party can never confirm a transaction.
          numConfirmingParticipants == 0 || threshold.value <= numConfirmingParticipants,
          (),
          s"Party $partyId cannot meet threshold of $threshold confirming participants with participants $participants",
        )
        .map(_ => PartyToParticipant(partyId, threshold, participants))
    }

    noDuplicatePParticipants.flatMap(_ => thresholdCanBeMet)
  }

  def tryCreate(
      partyId: PartyId,
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
  ): PartyToParticipant =
    create(partyId, threshold, participants).valueOr(err => throw new IllegalArgumentException(err))

  def uniqueKey(partyId: PartyId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(partyId.toProtoPrimitive))

  def code: Code = Code.PartyToParticipant

  def fromProtoV30(
      value: v30.PartyToParticipant
  ): ParsingResult[PartyToParticipant] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt("threshold", value.threshold)
      participants <- value.participants.traverse(HostingParticipant.fromProtoV30)
    } yield PartyToParticipant(partyId, threshold, participants)
}

/** Dynamic domain parameter settings for the domain
  *
  * Each domain has a set of parameters that can be changed at runtime.
  * These changes are authorized by the owner of the domain and distributed
  * to all nodes accordingly.
  */
final case class DomainParametersState(domain: DomainId, parameters: DynamicDomainParameters)
    extends TopologyMapping {

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.DomainParametersState(
        v30.DomainParametersState(
          domain = domain.toProtoPrimitive,
          domainParameters = Some(parameters.toProtoV30),
        )
      )
    )

  def code: TopologyMapping.Code = Code.DomainParametersState

  override def namespace: Namespace = domain.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = DomainParametersState.uniqueKey(domain)
}

object DomainParametersState {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMapping.Code = Code.DomainParametersState

  def fromProtoV30(
      value: v30.DomainParametersState
  ): ParsingResult[DomainParametersState] = {
    val v30.DomainParametersState(domainIdP, domainParametersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      parameters <- ProtoConverter.parseRequired(
        DynamicDomainParameters.fromProtoV30,
        "domainParameters",
        domainParametersP,
      )
    } yield DomainParametersState(domainId, parameters)
  }
}

/** Dynamic sequencing parameter settings for the domain
  *
  * Each domain has a set of sequencing parameters that can be changed at runtime.
  * These changes are authorized by the owner of the domain and distributed
  * to all nodes accordingly.
  */
final case class DynamicSequencingParametersState(
    domain: DomainId,
    parameters: DynamicSequencingParameters,
) extends TopologyMapping {

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SequencingDynamicParametersState(
        v30.DynamicSequencingParametersState(
          domain = domain.toProtoPrimitive,
          sequencingParameters = Some(parameters.toProtoV30),
        )
      )
    )

  def code: TopologyMapping.Code = Code.SequencingDynamicParametersState

  override def namespace: Namespace = domain.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = DomainParametersState.uniqueKey(domain)
}

object DynamicSequencingParametersState {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMapping.Code = Code.SequencingDynamicParametersState

  def fromProtoV30(
      value: v30.DynamicSequencingParametersState
  ): ParsingResult[DynamicSequencingParametersState] = {
    val v30.DynamicSequencingParametersState(domainIdP, sequencingParametersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      representativeProtocolVersion <- DynamicSequencingParameters.protocolVersionRepresentativeFor(
        ProtoVersion(30)
      )
      parameters <- sequencingParametersP
        .map(DynamicSequencingParameters.fromProtoV30)
        .getOrElse(Right(DynamicSequencingParameters.default(representativeProtocolVersion)))
    } yield DynamicSequencingParametersState(domainId, parameters)
  }
}

/** Mediator definition for a domain
  *
  * Each domain needs at least one mediator (group), but can have multiple.
  * Mediators can be temporarily turned off by making them observers. This way,
  * they get informed but they don't have to reply.
  */
final case class MediatorDomainState private (
    domain: DomainId,
    group: MediatorGroupIndex,
    threshold: PositiveInt,
    active: NonEmpty[Seq[MediatorId]],
    observers: Seq[MediatorId],
) extends TopologyMapping {

  lazy val allMediatorsInGroup: NonEmpty[Seq[MediatorId]] = active ++ observers

  def toProto: v30.MediatorDomainState =
    v30.MediatorDomainState(
      domain = domain.toProtoPrimitive,
      group = group.unwrap,
      threshold = threshold.unwrap,
      active = active.map(_.uid.toProtoPrimitive),
      observers = observers.map(_.uid.toProtoPrimitive),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.MediatorDomainState(
        toProto
      )
    )

  override def code: TopologyMapping.Code = Code.MediatorDomainState

  override def namespace: Namespace = domain.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = MediatorDomainState.uniqueKey(domain, group)
}

object MediatorDomainState {

  def uniqueKey(domainId: DomainId, group: MediatorGroupIndex): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive).add(group.unwrap))

  def code: TopologyMapping.Code = Code.MediatorDomainState

  def create(
      domain: DomainId,
      group: MediatorGroupIndex,
      threshold: PositiveInt,
      active: Seq[MediatorId],
      observers: Seq[MediatorId],
  ): Either[String, MediatorDomainState] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of mediator domain state higher than number of mediators ${active.length}",
    )
    mediatorsBothActiveAndObserver = active.intersect(observers)
    _ <- Either.cond(
      mediatorsBothActiveAndObserver.isEmpty,
      (),
      s"the following mediators were defined both as active and observer: ${mediatorsBothActiveAndObserver
          .mkString(", ")}",
    )
    activeNE <- NonEmpty
      .from(active.distinct)
      .toRight("mediator domain state requires at least one active mediator")
  } yield MediatorDomainState(domain, group, threshold, activeNE, observers.distinct)

  def fromProtoV30(
      value: v30.MediatorDomainState
  ): ParsingResult[MediatorDomainState] = {
    val v30.MediatorDomainState(domainIdP, groupP, thresholdP, activeP, observersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      group <- NonNegativeInt
        .create(groupP)
        .leftMap(ProtoDeserializationError.InvariantViolation("group", _))
      threshold <- ProtoConverter.parsePositiveInt("threshold", thresholdP)
      active <- activeP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "active").map(MediatorId(_))
      )
      observers <- observersP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "observers").map(MediatorId(_))
      )
      result <- create(domainId, group, threshold, active, observers).leftMap(
        ProtoDeserializationError.OtherError.apply
      )
    } yield result
  }

}

/** which sequencers are active on the given domain
  *
  * authorization: whoever controls the domain and all the owners of the active or observing sequencers that
  *   were not already present in the tx with serial = n - 1
  *   exception: a sequencer can leave the consortium unilaterally as long as there are enough members
  *              to reach the threshold
  * UNIQUE(domain)
  */
final case class SequencerDomainState private (
    domain: DomainId,
    threshold: PositiveInt,
    active: NonEmpty[Seq[SequencerId]],
    observers: Seq[SequencerId],
) extends TopologyMapping {

  lazy val allSequencers: NonEmpty[Seq[SequencerId]] = active ++ observers

  def toProto: v30.SequencerDomainState =
    v30.SequencerDomainState(
      domain = domain.toProtoPrimitive,
      threshold = threshold.unwrap,
      active = active.map(_.uid.toProtoPrimitive),
      observers = observers.map(_.uid.toProtoPrimitive),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SequencerDomainState(
        toProto
      )
    )

  def code: TopologyMapping.Code = Code.SequencerDomainState

  override def namespace: Namespace = domain.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = SequencerDomainState.uniqueKey(domain)
}

object SequencerDomainState {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMapping.Code = Code.SequencerDomainState

  def create(
      domain: DomainId,
      threshold: PositiveInt,
      active: Seq[SequencerId],
      observers: Seq[SequencerId],
  ): Either[String, SequencerDomainState] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of sequencer domain state higher than number of active sequencers ${active.length}",
    )
    sequencersBothActiveAndObserver = active.intersect(observers)
    _ <- Either.cond(
      sequencersBothActiveAndObserver.isEmpty,
      (),
      s"the following sequencers were defined both as active and observer: ${sequencersBothActiveAndObserver
          .mkString(", ")}",
    )
    activeNE <- NonEmpty
      .from(active.distinct)
      .toRight("sequencer domain state requires at least one active sequencer")
  } yield SequencerDomainState(domain, threshold, activeNE, observers.distinct)

  def fromProtoV30(
      value: v30.SequencerDomainState
  ): ParsingResult[SequencerDomainState] = {
    val v30.SequencerDomainState(domainIdP, thresholdP, activeP, observersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      threshold <- ProtoConverter.parsePositiveInt("threshold", thresholdP)
      active <- activeP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "active").map(SequencerId(_))
      )
      observers <- observersP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "observers").map(SequencerId(_))
      )
      result <- create(domainId, threshold, active, observers).leftMap(
        ProtoDeserializationError.OtherError.apply
      )
    } yield result
  }

}

// Purge topology transaction-x
final case class PurgeTopologyTransaction private (
    domain: DomainId,
    mappings: NonEmpty[Seq[TopologyMapping]],
) extends TopologyMapping {

  def toProto: v30.PurgeTopologyTransaction =
    v30.PurgeTopologyTransaction(
      domain = domain.toProtoPrimitive,
      mappings = mappings.map(_.toProtoV30),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PurgeTopologyTxs(
        toProto
      )
    )

  def code: TopologyMapping.Code = Code.PurgeTopologyTransaction

  override def namespace: Namespace = domain.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = PurgeTopologyTransaction.uniqueKey(domain)
}

object PurgeTopologyTransaction {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMapping.Code = Code.PurgeTopologyTransaction

  def create(
      domain: DomainId,
      mappings: Seq[TopologyMapping],
  ): Either[String, PurgeTopologyTransaction] = for {
    mappingsToPurge <- NonEmpty
      .from(mappings)
      .toRight("purge topology transaction-x requires at least one topology mapping")
  } yield PurgeTopologyTransaction(domain, mappingsToPurge)

  def fromProtoV30(
      value: v30.PurgeTopologyTransaction
  ): ParsingResult[PurgeTopologyTransaction] = {
    val v30.PurgeTopologyTransaction(domainIdP, mappingsP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      mappings <- mappingsP.traverse(TopologyMapping.fromProtoV30)
      result <- create(domainId, mappings).leftMap(
        ProtoDeserializationError.OtherError.apply
      )
    } yield result
  }

}
