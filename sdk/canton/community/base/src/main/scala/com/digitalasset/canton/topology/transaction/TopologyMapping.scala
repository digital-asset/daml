// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.Monoid
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
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

import scala.math.Ordering.Implicits.*
import scala.reflect.ClassTag

sealed trait TopologyMapping extends Product with Serializable with PrettyPrinting {

  require(maybeUid.forall(_.namespace == namespace), "namespace is inconsistent")

  override def pretty: Pretty[this.type] = adHocPrettyInstance

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

  sealed case class Code private (dbInt: Int, code: String)
  object Code {

    object NamespaceDelegation extends Code(1, "nsd")
    object IdentifierDelegation extends Code(2, "idd")
    object DecentralizedNamespaceDefinition extends Code(3, "dnd")

    object OwnerToKeyMapping extends Code(4, "otk")

    object DomainTrustCertificate extends Code(5, "dtc")
    object ParticipantDomainPermission extends Code(6, "pdp")
    object PartyHostingLimits extends Code(7, "phl")
    object VettedPackages extends Code(8, "vtp")

    object PartyToParticipant extends Code(9, "ptp")
    object AuthorityOf extends Code(10, "auo")

    object DomainParametersState extends Code(11, "dop")
    object MediatorDomainState extends Code(12, "mds")
    object SequencerDomainState extends Code(13, "sds")
    object OffboardParticipant extends Code(14, "ofp")

    object PurgeTopologyTransaction extends Code(15, "ptt")
    object TrafficControlState extends Code(16, "tcs")

    object SequencingDynamicParametersState extends Code(17, "sep")

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
      AuthorityOf,
      DomainParametersState,
      MediatorDomainState,
      SequencerDomainState,
      OffboardParticipant,
      PurgeTopologyTransaction,
      TrafficControlState,
    )

    def fromString(code: String): ParsingResult[Code] =
      all
        .find(_.code == code)
        .toRight(UnrecognizedEnum("TopologyMapping.Code", code, all.map(_.code)))

    implicit val setParameterTopologyMappingCode: SetParameter[Code] =
      (v, pp) => pp.setInt(v.dbInt)

  }

  // Small wrapper to not have to work with (Set[Namespace], Set[Namespace], Set[Uid])
  final case class RequiredAuthAuthorizations(
      namespacesWithRoot: Set[Namespace] = Set.empty,
      namespaces: Set[Namespace] = Set.empty,
      uids: Set[UniqueIdentifier] = Set.empty,
      extraKeys: Set[Fingerprint] = Set.empty,
  ) extends PrettyPrinting {
    def isEmpty: Boolean =
      namespacesWithRoot.isEmpty && namespaces.isEmpty && uids.isEmpty && extraKeys.isEmpty

    override def pretty: Pretty[RequiredAuthAuthorizations.this.type] = prettyOfClass(
      paramIfNonEmpty("namespacesWithRoot", _.namespacesWithRoot),
      paramIfNonEmpty("namespaces", _.namespaces),
      paramIfNonEmpty("uids", _.uids),
      paramIfNonEmpty("extraKeys", _.extraKeys),
    )
  }

  object RequiredAuthAuthorizations {

    val empty: RequiredAuthAuthorizations = RequiredAuthAuthorizations()

    implicit val monoid: Monoid[RequiredAuthAuthorizations] =
      new Monoid[RequiredAuthAuthorizations] {
        override def empty: RequiredAuthAuthorizations = RequiredAuthAuthorizations.empty

        override def combine(
            x: RequiredAuthAuthorizations,
            y: RequiredAuthAuthorizations,
        ): RequiredAuthAuthorizations =
          RequiredAuthAuthorizations(
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
        provided: RequiredAuthAuthorizations
    ): Either[RequiredAuthAuthorizations, Unit]

    final def or(next: RequiredAuth): RequiredAuth =
      RequiredAuth.Or(this, next)

    final def foldMap[T](
        namespaceCheck: RequiredNamespaces => T,
        uidCheck: RequiredUids => T,
    )(implicit T: Monoid[T]): T = {
      def loop(x: RequiredAuth): T = x match {
        case ns @ RequiredNamespaces(_, _) => namespaceCheck(ns)
        case uids @ RequiredUids(_, _) => uidCheck(uids)
        case EmptyAuthorization => T.empty
        case Or(first, second) => T.combine(loop(first), loop(second))
      }
      loop(this)
    }

    def authorizations: RequiredAuthAuthorizations
  }

  object RequiredAuth {

    private[transaction] case object EmptyAuthorization extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: RequiredAuthAuthorizations
      ): Either[RequiredAuthAuthorizations, Unit] = Either.unit

      override def authorizations: RequiredAuthAuthorizations = RequiredAuthAuthorizations()

      override def pretty: Pretty[EmptyAuthorization.this.type] = adHocPrettyInstance
    }

    final case class RequiredNamespaces(
        namespaces: Set[Namespace],
        override val requireRootDelegation: Boolean = false,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: RequiredAuthAuthorizations
      ): Either[RequiredAuthAuthorizations, Unit] = {
        val filter = if (requireRootDelegation) provided.namespacesWithRoot else provided.namespaces
        val missing = namespaces.filter(ns => !filter(ns))
        Either.cond(
          missing.isEmpty,
          (),
          RequiredAuthAuthorizations(
            namespacesWithRoot = if (requireRootDelegation) missing else Set.empty,
            namespaces = if (requireRootDelegation) Set.empty else missing,
          ),
        )
      }

      override def authorizations: RequiredAuthAuthorizations = RequiredAuthAuthorizations(
        namespacesWithRoot = if (requireRootDelegation) namespaces else Set.empty,
        namespaces = if (requireRootDelegation) Set.empty else namespaces,
      )

      override def pretty: Pretty[RequiredNamespaces.this.type] = prettyOfClass(
        unnamedParam(_.namespaces),
        paramIfTrue("requireRootDelegation", _.requireRootDelegation),
      )
    }

    final case class RequiredUids(
        uids: Set[UniqueIdentifier],
        extraKeys: Set[Fingerprint] = Set.empty,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: RequiredAuthAuthorizations
      ): Either[RequiredAuthAuthorizations, Unit] = {
        val missingUids =
          uids.filter(uid => !provided.uids(uid) && !provided.namespaces(uid.namespace))
        val missingExtraKeys = extraKeys -- provided.extraKeys
        val missingAuth =
          RequiredAuthAuthorizations(uids = missingUids, extraKeys = missingExtraKeys)
        Either.cond(
          missingAuth.isEmpty,
          (),
          missingAuth,
        )
      }

      override def authorizations: RequiredAuthAuthorizations = RequiredAuthAuthorizations(
        uids = uids,
        extraKeys = extraKeys,
      )

      override def pretty: Pretty[RequiredUids.this.type] = prettyOfClass(
        paramIfNonEmpty("uids", _.uids),
        paramIfNonEmpty("extraKeys", _.extraKeys),
      )
    }

    private[transaction] final case class Or(
        first: RequiredAuth,
        second: RequiredAuth,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: RequiredAuthAuthorizations
      ): Either[RequiredAuthAuthorizations, Unit] =
        first
          .satisfiedByActualAuthorizers(provided)
          .orElse(second.satisfiedByActualAuthorizers(provided))

      override def authorizations: RequiredAuthAuthorizations =
        RequiredAuthAuthorizations.monoid.combine(first.authorizations, second.authorizations)

      override def pretty: Pretty[Or.this.type] =
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
      case Mapping.DomainTrustCertificate(value) => DomainTrustCertificate.fromProtoV30(value)
      case Mapping.PartyHostingLimits(value) => PartyHostingLimits.fromProtoV30(value)
      case Mapping.ParticipantPermission(value) => ParticipantDomainPermission.fromProtoV30(value)
      case Mapping.VettedPackages(value) => VettedPackages.fromProtoV30(value)
      case Mapping.PartyToParticipant(value) => PartyToParticipant.fromProtoV30(value)
      case Mapping.AuthorityOf(value) => AuthorityOf.fromProtoV30(value)
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
  ): RequiredAuth = {
    // All namespace delegation creations require the root delegation privilege.
    RequiredNamespaces(Set(namespace), requireRootDelegation = true)
  }

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
  def isRootCertificate(sit: GenericSignedTopologyTransaction): Boolean = {
    ((sit.operation == TopologyChangeOp.Replace && sit.serial == PositiveInt.one) ||
      (sit.operation == TopologyChangeOp.Remove && sit.serial != PositiveInt.one)) &&
    sit.mapping
      .select[transaction.NamespaceDelegation]
      .exists(ns =>
        sit.signatures.size == 1 &&
          sit.signatures.head1.signedBy == ns.namespace.fingerprint &&
          ns.isRootDelegation &&
          ns.target.fingerprint == ns.namespace.fingerprint
      )
  }

  /** Returns true if the given transaction is a root delegation */
  def isRootDelegation(sit: GenericSignedTopologyTransaction): Boolean = {
    isRootCertificate(sit) || (
      sit.operation == TopologyChangeOp.Replace &&
        sit.mapping
          .select[transaction.NamespaceDelegation]
          .exists(ns => ns.isRootDelegation)
    )
  }

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
  ): RequiredAuth = {
    previous match {
      case None =>
        RequiredNamespaces(owners.forgetNE)
      case Some(
            TopologyTransaction(
              _op,
              _serial,
              DecentralizedNamespaceDefinition(`namespace`, _previousThreshold, previousOwners),
            )
          ) =>
        val added = owners.diff(previousOwners)
        // all added owners and the quorum of existing owners MUST sign
        RequiredNamespaces(added + namespace)
      case Some(_topoTx) =>
        // TODO(#14048): proper error or ignore
        sys.error(s"unexpected transaction data: $previous")
    }
  }

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
        s"Invalid threshold (${threshold}) for ${decentralizedNamespace} with ${owners.size} owners",
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
      threshold <- ProtoConverter.parsePositiveInt(thresholdP)
      owners <- ownersP.traverse(Fingerprint.fromProtoPrimitive)
      ownersNE <- NonEmpty
        .from(owners.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            "owners cannot be empty"
          )
        )
      item <- create(decentralizedNamespace, threshold, ownersNE.map(Namespace(_)))
        .leftMap(ProtoDeserializationError.OtherError)
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
  ): RequiredAuth = RequiredUids(Set(identifier))

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
    domain: Option[DomainId],
    keys: NonEmpty[Seq[PublicKey]],
) extends TopologyMapping {

  def toProto: v30.OwnerToKeyMapping = v30.OwnerToKeyMapping(
    member = member.toProtoPrimitive,
    publicKeys = keys.map(_.toProtoPublicKeyV30),
    domain = domain.map(_.toProtoPrimitive).getOrElse(""),
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

  override def restrictedToDomain: Option[DomainId] = domain

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

  override def uniqueKey: MappingHash = OwnerToKeyMapping.uniqueKey(member, domain)
}

object OwnerToKeyMapping {

  def uniqueKey(member: Member, domain: Option[DomainId]): MappingHash =
    TopologyMapping.buildUniqueKey(code)(b =>
      TopologyMapping.addDomainId(b.add(member.uid.toProtoPrimitive), domain)
    )

  def code: TopologyMapping.Code = Code.OwnerToKeyMapping

  def fromProtoV30(
      value: v30.OwnerToKeyMapping
  ): ParsingResult[OwnerToKeyMapping] = {
    val v30.OwnerToKeyMapping(memberP, keysP, domainP) = value
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      keys <- keysP.traverse(x =>
        ProtoConverter
          .parseRequired(PublicKey.fromProtoPublicKeyV30, "public_keys", Some(x))
      )
      keysNE <- NonEmpty
        .from(keys)
        .toRight(ProtoDeserializationError.FieldNotSet("public_keys"): ProtoDeserializationError)
      domain <- OptionUtil
        .emptyStringAsNone(domainP)
        .traverse(DomainId.fromProtoPrimitive(_, "domain"))
    } yield OwnerToKeyMapping(member, domain, keysNE)
  }

}

/** Participant domain trust certificate
  */
final case class DomainTrustCertificate(
    participantId: ParticipantId,
    domainId: DomainId,
    // TODO(#15399): respect this restriction when reassigning contracts
    transferOnlyToGivenTargetDomains: Boolean,
    targetDomains: Seq[DomainId],
) extends TopologyMapping {

  def toProto: v30.DomainTrustCertificate =
    v30.DomainTrustCertificate(
      participant = participantId.toProtoPrimitive,
      domain = domainId.toProtoPrimitive,
      transferOnlyToGivenTargetDomains = transferOnlyToGivenTargetDomains,
      targetDomains = targetDomains.map(_.toProtoPrimitive),
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
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      transferOnlyToGivenTargetDomains = value.transferOnlyToGivenTargetDomains
      targetDomains <- value.targetDomains.traverse(
        DomainId.fromProtoPrimitive(_, "target_domains")
      )
    } yield DomainTrustCertificate(
      participantId,
      domainId,
      transferOnlyToGivenTargetDomains,
      targetDomains,
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

final case class ParticipantDomainLimits(
    confirmationRequestsMaxRate: Int,
    maxNumParties: Int,
    maxNumPackages: Int,
) {
  def toProto: v30.ParticipantDomainLimits =
    v30.ParticipantDomainLimits(confirmationRequestsMaxRate, maxNumParties, maxNumPackages)
}
object ParticipantDomainLimits {
  def fromProtoV30(value: v30.ParticipantDomainLimits): ParticipantDomainLimits =
    ParticipantDomainLimits(
      value.confirmationRequestsMaxRate,
      value.maxNumParties,
      value.maxNumPackages,
    )
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
      participant = participantId.toProtoPrimitive,
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
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      permission <- ParticipantPermission.fromProtoV30(value.permission)
      limits = value.limits.map(ParticipantDomainLimits.fromProtoV30)
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
    quota: Int,
) extends TopologyMapping {

  def toProto: v30.PartyHostingLimits =
    v30.PartyHostingLimits(
      domain = domainId.toProtoPrimitive,
      party = partyId.toProtoPrimitive,
      quota = quota,
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
      quota = value.quota
    } yield PartyHostingLimits(domainId, partyId, quota)
}

// Package vetting
final case class VettedPackages(
    participantId: ParticipantId,
    domainId: Option[DomainId],
    packageIds: Seq[LfPackageId],
) extends TopologyMapping {

  def toProto: v30.VettedPackages =
    v30.VettedPackages(
      participant = participantId.toProtoPrimitive,
      packageIds = packageIds,
      domain = domainId.fold("")(_.toProtoPrimitive),
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

  override def restrictedToDomain: Option[DomainId] = domainId

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredUids(Set(participantId.uid))

  override def uniqueKey: MappingHash = VettedPackages.uniqueKey(participantId, domainId)
}

object VettedPackages {

  def uniqueKey(participantId: ParticipantId, domainId: Option[DomainId]): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(participantId.toProtoPrimitive).add(domainId.fold("")(_.toProtoPrimitive))
    )

  def code: Code = Code.VettedPackages

  def fromProtoV30(
      value: v30.VettedPackages
  ): ParsingResult[VettedPackages] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      packageIds <- value.packageIds
        .traverse(LfPackageId.fromString)
        .leftMap(ProtoDeserializationError.ValueConversionError("package_ids", _))
      domainId <-
        if (value.domain.nonEmpty)
          DomainId.fromProtoPrimitive(value.domain, "domain").map(_.some)
        else Right(None)
    } yield VettedPackages(participantId, domainId, packageIds)
}

// Party to participant mappings
final case class HostingParticipant(
    participantId: ParticipantId,
    permission: ParticipantPermission,
) {
  def toProto: v30.PartyToParticipant.HostingParticipant =
    v30.PartyToParticipant.HostingParticipant(
      participant = participantId.toProtoPrimitive,
      permission = permission.toProtoV30,
    )
}

object HostingParticipant {
  def fromProtoV30(
      value: v30.PartyToParticipant.HostingParticipant
  ): ParsingResult[HostingParticipant] = for {
    participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
    permission <- ParticipantPermission.fromProtoV30(value.permission)
  } yield HostingParticipant(participantId, permission)
}

final case class PartyToParticipant private (
    partyId: PartyId,
    domainId: Option[DomainId],
    threshold: PositiveInt,
    participants: Seq[HostingParticipant],
    groupAddressing: Boolean,
) extends TopologyMapping {

  def toProto: v30.PartyToParticipant =
    v30.PartyToParticipant(
      party = partyId.toProtoPrimitive,
      threshold = threshold.value,
      participants = participants.map(_.toProto),
      groupAddressing = groupAddressing,
      domain = domainId.fold("")(_.toProtoPrimitive),
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

  override def restrictedToDomain: Option[DomainId] = domainId

  def participantIds: Seq[ParticipantId] = participants.map(_.participantId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = {
    previous
      .collect {
        case TopologyTransaction(
              TopologyChangeOp.Replace,
              _,
              PartyToParticipant(_, _, prevThreshold, prevParticipants, prevGroupAddressing),
            ) =>
          val current = this
          val currentParticipantIds = participants.map(_.participantId.uid).toSet
          val prevParticipantIds = prevParticipants.map(_.participantId.uid).toSet
          val removedParticipants = prevParticipantIds -- currentParticipantIds
          val addedParticipants = currentParticipantIds -- prevParticipantIds

          val contentHasChanged =
            prevGroupAddressing != current.groupAddressing || prevThreshold != current.threshold

          // check whether a participant can unilaterally unhost a party
          if (
            // no change in group addressing or threshold
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
  }

  override def uniqueKey: MappingHash = PartyToParticipant.uniqueKey(partyId, domainId)
}

object PartyToParticipant {

  def create(
      partyId: PartyId,
      domainId: Option[DomainId],
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
      groupAddressing: Boolean,
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
        .map(_ => PartyToParticipant(partyId, domainId, threshold, participants, groupAddressing))
    }

    noDuplicatePParticipants.flatMap(_ => thresholdCanBeMet)
  }

  def tryCreate(
      partyId: PartyId,
      domainId: Option[DomainId],
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
      groupAddressing: Boolean,
  ): PartyToParticipant =
    create(partyId, domainId, threshold, participants, groupAddressing).valueOr(err =>
      throw new IllegalArgumentException(err)
    )

  def uniqueKey(partyId: PartyId, domainId: Option[DomainId]): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(partyId.toProtoPrimitive).add(domainId.fold("")(_.toProtoPrimitive))
    )

  def code: Code = Code.PartyToParticipant

  def fromProtoV30(
      value: v30.PartyToParticipant
  ): ParsingResult[PartyToParticipant] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt(value.threshold)
      participants <- value.participants.traverse(HostingParticipant.fromProtoV30)
      groupAddressing = value.groupAddressing
      domainId <-
        if (value.domain.nonEmpty)
          DomainId.fromProtoPrimitive(value.domain, "domain").map(_.some)
        else Right(None)
    } yield PartyToParticipant(partyId, domainId, threshold, participants, groupAddressing)
}

// AuthorityOf
final case class AuthorityOf private (
    partyId: PartyId,
    domainId: Option[DomainId],
    threshold: PositiveInt,
    parties: Seq[PartyId],
) extends TopologyMapping {

  def toProto: v30.AuthorityOf =
    v30.AuthorityOf(
      party = partyId.toProtoPrimitive,
      threshold = threshold.unwrap,
      parties = parties.map(_.toProtoPrimitive),
      domain = domainId.fold("")(_.toProtoPrimitive),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.AuthorityOf(
        toProto
      )
    )

  override def code: Code = Code.AuthorityOf

  override def namespace: Namespace = partyId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToDomain: Option[DomainId] = domainId

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = {
    // TODO(#12390): take the previous transaction into account
    RequiredUids(Set(partyId.uid) ++ parties.map(_.uid))
  }

  override def uniqueKey: MappingHash = AuthorityOf.uniqueKey(partyId, domainId)
}

object AuthorityOf {

  def create(
      partyId: PartyId,
      domainId: Option[DomainId],
      threshold: PositiveInt,
      parties: Seq[PartyId],
  ): Either[String, AuthorityOf] = {
    Either
      .cond(
        threshold.value <= parties.size,
        (),
        s"Invalid threshold $threshold for $partyId with authorizers $parties",
      )
      .map(_ => AuthorityOf(partyId, domainId, threshold, parties))
  }

  def uniqueKey(partyId: PartyId, domainId: Option[DomainId]): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(partyId.toProtoPrimitive).add(domainId.fold("")(_.toProtoPrimitive))
    )

  def code: Code = Code.AuthorityOf

  def fromProtoV30(
      value: v30.AuthorityOf
  ): ParsingResult[AuthorityOf] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt(value.threshold)
      parties <- value.parties.traverse(PartyId.fromProtoPrimitive(_, "parties"))
      domainId <-
        if (value.domain.nonEmpty)
          DomainId.fromProtoPrimitive(value.domain, "domain").map(_.some)
        else Right(None)
      authorityOf <- create(partyId, domainId, threshold, parties)
        .leftMap(ProtoDeserializationError.OtherError)
    } yield authorityOf
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
  * Mediators can be temporarily be turned off by making them observers. This way,
  * they get informed but they don't have to reply.
  */
final case class MediatorDomainState private (
    domain: DomainId,
    group: NonNegativeInt,
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

  def uniqueKey(domainId: DomainId, group: NonNegativeInt): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive).add(group.unwrap))

  def code: TopologyMapping.Code = Code.MediatorDomainState

  def create(
      domain: DomainId,
      group: NonNegativeInt,
      threshold: PositiveInt,
      active: Seq[MediatorId],
      observers: Seq[MediatorId],
  ): Either[String, MediatorDomainState] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of mediator domain state higher than number of mediators ${active.length}",
    )
    activeNE <- NonEmpty
      .from(active)
      .toRight("mediator domain state requires at least one active mediator")
  } yield MediatorDomainState(domain, group, threshold, activeNE, observers)

  def fromProtoV30(
      value: v30.MediatorDomainState
  ): ParsingResult[MediatorDomainState] = {
    val v30.MediatorDomainState(domainIdP, groupP, thresholdP, activeP, observersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      group <- NonNegativeInt
        .create(groupP)
        .leftMap(ProtoDeserializationError.InvariantViolation(_))
      threshold <- ProtoConverter.parsePositiveInt(thresholdP)
      active <- activeP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "active").map(MediatorId(_))
      )
      observers <- observersP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "observers").map(MediatorId(_))
      )
      result <- create(domainId, group, threshold, active, observers).leftMap(
        ProtoDeserializationError.OtherError
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
    activeNE <- NonEmpty
      .from(active)
      .toRight("sequencer domain state requires at least one active sequencer")
  } yield SequencerDomainState(domain, threshold, activeNE, observers)

  def fromProtoV30(
      value: v30.SequencerDomainState
  ): ParsingResult[SequencerDomainState] = {
    val v30.SequencerDomainState(domainIdP, thresholdP, activeP, observersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      threshold <- ProtoConverter.parsePositiveInt(thresholdP)
      active <- activeP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "active").map(SequencerId(_))
      )
      observers <- observersP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "observers").map(SequencerId(_))
      )
      result <- create(domainId, threshold, active, observers).leftMap(
        ProtoDeserializationError.OtherError
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
        ProtoDeserializationError.OtherError
      )
    } yield result
  }

}
