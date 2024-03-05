// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.Monoid
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  UnrecognizedEnum,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30.Enums
import com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping
import com.digitalasset.canton.protocol.{DynamicDomainParameters, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.RequiredAuthX.*
import com.digitalasset.canton.topology.transaction.TopologyMappingX.{
  Code,
  MappingHash,
  RequiredAuthX,
}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.SetParameter

import scala.math.Ordering.Implicits.*
import scala.reflect.ClassTag

sealed trait TopologyMappingX extends Product with Serializable with PrettyPrinting {

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
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX

  def restrictedToDomain: Option[DomainId]

  def toProtoV30: v30.TopologyMapping

  def uniqueKey: MappingHash

  final def select[TargetMapping <: TopologyMappingX](implicit
      M: ClassTag[TargetMapping]
  ): Option[TargetMapping] = M.unapply(this)

}

object TopologyMappingX {

  private[transaction] def buildUniqueKey(code: TopologyMappingX.Code)(
      addUniqueKeyToBuilder: HashBuilder => HashBuilder
  ): MappingHash =
    MappingHash(
      addUniqueKeyToBuilder(
        Hash.build(HashPurpose.DomainTopologyTransactionMessageSignature, HashAlgorithm.Sha256)
      ).add(code.dbInt)
        .finish()
    )

  final case class MappingHash(hash: Hash) extends AnyVal

  sealed case class Code private (dbInt: Int, code: String)
  object Code {

    object NamespaceDelegationX extends Code(1, "nsd")
    object IdentifierDelegationX extends Code(2, "idd")
    object DecentralizedNamespaceDefinitionX extends Code(3, "dnd")

    object OwnerToKeyMappingX extends Code(4, "otk")

    object DomainTrustCertificateX extends Code(5, "dtc")
    object ParticipantDomainPermissionX extends Code(6, "pdp")
    object PartyHostingLimitsX extends Code(7, "phl")
    object VettedPackagesX extends Code(8, "vtp")

    object PartyToParticipantX extends Code(9, "ptp")
    object AuthorityOfX extends Code(10, "auo")

    object DomainParametersStateX extends Code(11, "dop")
    object MediatorDomainStateX extends Code(12, "mds")
    object SequencerDomainStateX extends Code(13, "sds")
    object OffboardParticipantX extends Code(14, "ofp")

    object PurgeTopologyTransactionX extends Code(15, "ptt")
    object TrafficControlStateX extends Code(16, "tcs")

    lazy val all: Seq[Code] = Seq(
      NamespaceDelegationX,
      IdentifierDelegationX,
      DecentralizedNamespaceDefinitionX,
      OwnerToKeyMappingX,
      DomainTrustCertificateX,
      ParticipantDomainPermissionX,
      VettedPackagesX,
      PartyToParticipantX,
      AuthorityOfX,
      DomainParametersStateX,
      MediatorDomainStateX,
      SequencerDomainStateX,
      OffboardParticipantX,
      PurgeTopologyTransactionX,
      TrafficControlStateX,
    )

    implicit val setParameterTopologyMappingCode: SetParameter[Code] =
      (v, pp) => pp.setInt(v.dbInt)

  }

  // Small wrapper to not have to work with (Set[Namespace], Set[Namespace], Set[Uid])
  final case class RequiredAuthXAuthorizations(
      namespacesWithRoot: Set[Namespace] = Set.empty,
      namespaces: Set[Namespace] = Set.empty,
      uids: Set[UniqueIdentifier] = Set.empty,
  ) extends PrettyPrinting {
    def isEmpty: Boolean = namespacesWithRoot.isEmpty && namespaces.isEmpty && uids.isEmpty

    override def pretty: Pretty[RequiredAuthXAuthorizations.this.type] = prettyOfClass(
      paramIfNonEmpty("namespacesWithRoot", _.namespacesWithRoot),
      paramIfNonEmpty("namespaces", _.namespaces),
      paramIfNonEmpty("uids", _.uids),
    )
  }

  object RequiredAuthXAuthorizations {

    val empty: RequiredAuthXAuthorizations = RequiredAuthXAuthorizations()

    implicit val monoid: Monoid[RequiredAuthXAuthorizations] =
      new Monoid[RequiredAuthXAuthorizations] {
        override def empty: RequiredAuthXAuthorizations = RequiredAuthXAuthorizations.empty

        override def combine(
            x: RequiredAuthXAuthorizations,
            y: RequiredAuthXAuthorizations,
        ): RequiredAuthXAuthorizations =
          RequiredAuthXAuthorizations(
            namespacesWithRoot = x.namespacesWithRoot ++ y.namespacesWithRoot,
            namespaces = x.namespaces ++ y.namespaces,
            uids = x.uids ++ y.uids,
          )
      }
  }

  sealed trait RequiredAuthX extends PrettyPrinting {
    def requireRootDelegation: Boolean = false
    def satisfiedByActualAuthorizers(
        namespacesWithRoot: Set[Namespace],
        namespaces: Set[Namespace],
        uids: Set[UniqueIdentifier],
    ): Either[RequiredAuthXAuthorizations, Unit]

    final def or(next: RequiredAuthX): RequiredAuthX =
      RequiredAuthX.Or(this, next)

    final def foldMap[T](
        namespaceCheck: RequiredNamespaces => T,
        uidCheck: RequiredUids => T,
    )(implicit T: Monoid[T]): T = {
      def loop(x: RequiredAuthX): T = x match {
        case ns @ RequiredNamespaces(_, _) => namespaceCheck(ns)
        case uids @ RequiredUids(_) => uidCheck(uids)
        case EmptyAuthorization => T.empty
        case Or(first, second) => T.combine(loop(first), loop(second))
      }
      loop(this)
    }

    def authorizations: RequiredAuthXAuthorizations
  }

  object RequiredAuthX {

    private[transaction] case object EmptyAuthorization extends RequiredAuthX {
      override def satisfiedByActualAuthorizers(
          namespacesWithRoot: Set[Namespace],
          namespaces: Set[Namespace],
          uids: Set[UniqueIdentifier],
      ): Either[RequiredAuthXAuthorizations, Unit] = Either.unit

      override def authorizations: RequiredAuthXAuthorizations = RequiredAuthXAuthorizations()

      override def pretty: Pretty[EmptyAuthorization.this.type] = adHocPrettyInstance
    }

    final case class RequiredNamespaces(
        namespaces: Set[Namespace],
        override val requireRootDelegation: Boolean = false,
    ) extends RequiredAuthX {
      override def satisfiedByActualAuthorizers(
          providedNamespacesWithRoot: Set[Namespace],
          providedNamespaces: Set[Namespace],
          uids: Set[UniqueIdentifier],
      ): Either[RequiredAuthXAuthorizations, Unit] = {
        val filter = if (requireRootDelegation) providedNamespacesWithRoot else providedNamespaces
        val missing = namespaces.filter(ns => !filter(ns))
        Either.cond(
          missing.isEmpty,
          (),
          RequiredAuthXAuthorizations(
            namespacesWithRoot = if (requireRootDelegation) missing else Set.empty,
            namespaces = if (requireRootDelegation) Set.empty else missing,
          ),
        )
      }

      override def authorizations: RequiredAuthXAuthorizations = RequiredAuthXAuthorizations(
        namespacesWithRoot = if (requireRootDelegation) namespaces else Set.empty,
        namespaces = if (requireRootDelegation) Set.empty else namespaces,
      )

      override def pretty: Pretty[RequiredNamespaces.this.type] = prettyOfClass(
        unnamedParam(_.namespaces),
        paramIfTrue("requireRootDelegation", _.requireRootDelegation),
      )
    }

    final case class RequiredUids(uids: Set[UniqueIdentifier]) extends RequiredAuthX {
      override def satisfiedByActualAuthorizers(
          namespacesWithRoot: Set[Namespace],
          namespaces: Set[Namespace],
          providedUids: Set[UniqueIdentifier],
      ): Either[RequiredAuthXAuthorizations, Unit] = {
        val missing = uids.filter(uid => !providedUids(uid) && !namespaces(uid.namespace))
        Either.cond(missing.isEmpty, (), RequiredAuthXAuthorizations(uids = missing))
      }

      override def authorizations: RequiredAuthXAuthorizations = RequiredAuthXAuthorizations(
        namespaces = uids.map(_.namespace),
        uids = uids,
      )

      override def pretty: Pretty[RequiredUids.this.type] = prettyOfClass(
        unnamedParam(_.uids)
      )
    }

    private[transaction] final case class Or(
        first: RequiredAuthX,
        second: RequiredAuthX,
    ) extends RequiredAuthX {
      override def satisfiedByActualAuthorizers(
          namespacesWithRoot: Set[Namespace],
          namespaces: Set[Namespace],
          uids: Set[UniqueIdentifier],
      ): Either[RequiredAuthXAuthorizations, Unit] =
        first
          .satisfiedByActualAuthorizers(namespacesWithRoot, namespaces, uids)
          .orElse(
            second
              .satisfiedByActualAuthorizers(namespacesWithRoot, namespaces, uids)
          )

      override def authorizations: RequiredAuthXAuthorizations =
        RequiredAuthXAuthorizations.monoid.combine(first.authorizations, second.authorizations)

      override def pretty: Pretty[Or.this.type] =
        prettyOfClass(unnamedParam(_.first), unnamedParam(_.second))
    }
  }

  def fromProtoV30(proto: v30.TopologyMapping): ParsingResult[TopologyMappingX] =
    proto.mapping match {
      case Mapping.Empty =>
        Left(ProtoDeserializationError.TransactionDeserialization("No mapping set"))
      case Mapping.NamespaceDelegation(value) => NamespaceDelegationX.fromProtoV30(value)
      case Mapping.IdentifierDelegation(value) => IdentifierDelegationX.fromProtoV30(value)
      case Mapping.DecentralizedNamespaceDefinition(value) =>
        DecentralizedNamespaceDefinitionX.fromProtoV30(value)
      case Mapping.OwnerToKeyMapping(value) => OwnerToKeyMappingX.fromProtoV30(value)
      case Mapping.DomainTrustCertificate(value) => DomainTrustCertificateX.fromProtoV30(value)
      case Mapping.PartyHostingLimits(value) => PartyHostingLimitsX.fromProtoV30(value)
      case Mapping.ParticipantPermission(value) => ParticipantDomainPermissionX.fromProtoV30(value)
      case Mapping.VettedPackages(value) => VettedPackagesX.fromProtoV30(value)
      case Mapping.PartyToParticipant(value) => PartyToParticipantX.fromProtoV30(value)
      case Mapping.AuthorityOf(value) => AuthorityOfX.fromProtoV30(value)
      case Mapping.DomainParametersState(value) => DomainParametersStateX.fromProtoV30(value)
      case Mapping.MediatorDomainState(value) => MediatorDomainStateX.fromProtoV30(value)
      case Mapping.SequencerDomainState(value) => SequencerDomainStateX.fromProtoV30(value)
      case Mapping.PurgeTopologyTxs(value) => PurgeTopologyTransactionX.fromProtoV30(value)
      case Mapping.TrafficControlState(value) => TrafficControlStateX.fromProtoV30(value)
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
final case class NamespaceDelegationX private (
    namespace: Namespace,
    target: SigningPublicKey,
    isRootDelegation: Boolean,
) extends TopologyMappingX {

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

  override def code: Code = Code.NamespaceDelegationX

  override def maybeUid: Option[UniqueIdentifier] = None

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = {
    // All namespace delegation creations require the root delegation privilege.
    RequiredNamespaces(Set(namespace), requireRootDelegation = true)
  }

  override lazy val uniqueKey: MappingHash =
    NamespaceDelegationX.uniqueKey(namespace, target.fingerprint)
}

object NamespaceDelegationX {

  def uniqueKey(namespace: Namespace, target: Fingerprint): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap).add(target.unwrap))

  def create(
      namespace: Namespace,
      target: SigningPublicKey,
      isRootDelegation: Boolean,
  ): Either[String, NamespaceDelegationX] =
    Either.cond(
      isRootDelegation || namespace.fingerprint != target.fingerprint,
      NamespaceDelegationX(namespace, target, isRootDelegation),
      s"Root certificate for $namespace needs to be set as isRootDelegation = true",
    )

  @VisibleForTesting
  protected[canton] def tryCreate(
      namespace: Namespace,
      target: SigningPublicKey,
      isRootDelegation: Boolean,
  ): NamespaceDelegationX =
    create(namespace, target, isRootDelegation).fold(err => sys.error(err), identity)

  def code: TopologyMappingX.Code = Code.NamespaceDelegationX

  /** Returns true if the given transaction is a self-signed root certificate */
  def isRootCertificate(sit: GenericSignedTopologyTransactionX): Boolean = {
    ((sit.operation == TopologyChangeOpX.Replace && sit.serial == PositiveInt.one) ||
      (sit.operation == TopologyChangeOpX.Remove && sit.serial != PositiveInt.one)) &&
    sit.mapping
      .select[transaction.NamespaceDelegationX]
      .exists(ns =>
        sit.signatures.size == 1 &&
          sit.signatures.head1.signedBy == ns.namespace.fingerprint &&
          ns.isRootDelegation &&
          ns.target.fingerprint == ns.namespace.fingerprint
      )
  }

  /** Returns true if the given transaction is a root delegation */
  def isRootDelegation(sit: GenericSignedTopologyTransactionX): Boolean = {
    isRootCertificate(sit) || (
      sit.operation == TopologyChangeOpX.Replace &&
        sit.mapping
          .select[transaction.NamespaceDelegationX]
          .exists(ns => ns.isRootDelegation)
    )
  }

  def fromProtoV30(
      value: v30.NamespaceDelegation
  ): ParsingResult[NamespaceDelegationX] =
    for {
      namespace <- Fingerprint.fromProtoPrimitive(value.namespace).map(Namespace(_))
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "target_key",
        value.targetKey,
      )
    } yield NamespaceDelegationX(namespace, target, value.isRootDelegation)

}

/** Defines a decentralized namespace
  *
  * authorization: whoever controls the domain and all the owners of the active or observing sequencers that
  *   were not already present in the tx with serial = n - 1
  *   exception: a sequencer can leave the consortium unilaterally as long as there are enough members
  *              to reach the threshold
  */
final case class DecentralizedNamespaceDefinitionX private (
    override val namespace: Namespace,
    threshold: PositiveInt,
    owners: NonEmpty[Set[Namespace]],
) extends TopologyMappingX {

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

  override def code: Code = Code.DecentralizedNamespaceDefinitionX

  override def maybeUid: Option[UniqueIdentifier] = None

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = {
    previous match {
      case None =>
        RequiredNamespaces(owners.forgetNE)
      case Some(
            TopologyTransactionX(
              _op,
              _serial,
              DecentralizedNamespaceDefinitionX(`namespace`, _previousThreshold, previousOwners),
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

  override def uniqueKey: MappingHash = DecentralizedNamespaceDefinitionX.uniqueKey(namespace)
}

object DecentralizedNamespaceDefinitionX {

  def uniqueKey(namespace: Namespace): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap))

  def code: TopologyMappingX.Code = Code.DecentralizedNamespaceDefinitionX

  def create(
      decentralizedNamespace: Namespace,
      threshold: PositiveInt,
      owners: NonEmpty[Set[Namespace]],
  ): Either[String, DecentralizedNamespaceDefinitionX] =
    for {
      _ <- Either.cond(
        owners.size >= threshold.value,
        (),
        s"Invalid threshold (${threshold}) for ${decentralizedNamespace} with ${owners.size} owners",
      )
    } yield DecentralizedNamespaceDefinitionX(decentralizedNamespace, threshold, owners)

  def fromProtoV30(
      value: v30.DecentralizedNamespaceDefinition
  ): ParsingResult[DecentralizedNamespaceDefinitionX] = {
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
    Namespace(Fingerprint(builder.finish().toLengthLimitedHexString))
  }
}

/** An identifier delegation
  *
  * entrusts a public-key to do any change with respect to the identifier
  * {(X,I) => p_k}
  */
final case class IdentifierDelegationX(identifier: UniqueIdentifier, target: SigningPublicKey)
    extends TopologyMappingX {

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

  override def code: Code = Code.IdentifierDelegationX

  override def namespace: Namespace = identifier.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(identifier)

  override def restrictedToDomain: Option[DomainId] = None

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(identifier))

  override def uniqueKey: MappingHash =
    IdentifierDelegationX.uniqueKey(identifier, target.fingerprint)
}

object IdentifierDelegationX {

  def uniqueKey(identifier: UniqueIdentifier, targetKey: Fingerprint): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(identifier.toProtoPrimitive).add(targetKey.unwrap))

  def code: Code = Code.IdentifierDelegationX

  def fromProtoV30(
      value: v30.IdentifierDelegation
  ): ParsingResult[IdentifierDelegationX] =
    for {
      identifier <- UniqueIdentifier.fromProtoPrimitive(value.uniqueIdentifier, "unique_identifier")
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "target_key",
        value.targetKey,
      )
    } yield IdentifierDelegationX(identifier, target)
}

/** A key owner (participant, mediator, sequencer) to key mapping
  *
  * In Canton, we need to know keys for all participating entities. The entities are
  * all the protocol members (participant, mediator) plus the
  * sequencer (which provides the communication infrastructure for the protocol members).
  */
final case class OwnerToKeyMappingX(
    member: Member,
    domain: Option[DomainId],
    keys: NonEmpty[Seq[PublicKey]],
) extends TopologyMappingX {

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

  def code: TopologyMappingX.Code = Code.OwnerToKeyMappingX

  override def namespace: Namespace = member.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(member.uid)

  override def restrictedToDomain: Option[DomainId] = domain

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(member.uid))

  override def uniqueKey: MappingHash = OwnerToKeyMappingX.uniqueKey(member, domain)
}

object OwnerToKeyMappingX {

  def uniqueKey(member: Member, domain: Option[DomainId]): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(b =>
      TopologyMappingX.addDomainId(b.add(member.uid.toProtoPrimitive), domain)
    )

  def code: TopologyMappingX.Code = Code.OwnerToKeyMappingX

  def fromProtoV30(
      value: v30.OwnerToKeyMapping
  ): ParsingResult[OwnerToKeyMappingX] = {
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
    } yield OwnerToKeyMappingX(member, domain, keysNE)
  }

}

/** Participant domain trust certificate
  */
final case class DomainTrustCertificateX(
    participantId: ParticipantId,
    domainId: DomainId,
    // TODO(#15399): respect this restriction when reassigning contracts
    transferOnlyToGivenTargetDomains: Boolean,
    targetDomains: Seq[DomainId],
) extends TopologyMappingX {

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

  override def code: Code = Code.DomainTrustCertificateX

  override def namespace: Namespace = participantId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX =
    RequiredUids(Set(participantId.uid))

  override def uniqueKey: MappingHash = DomainTrustCertificateX.uniqueKey(participantId, domainId)
}

object DomainTrustCertificateX {

  def uniqueKey(participantId: ParticipantId, domainId: DomainId): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(
      _.add(participantId.toProtoPrimitive).add(domainId.toProtoPrimitive)
    )

  def code: Code = Code.DomainTrustCertificateX

  def fromProtoV30(
      value: v30.DomainTrustCertificate
  ): ParsingResult[DomainTrustCertificateX] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      transferOnlyToGivenTargetDomains = value.transferOnlyToGivenTargetDomains
      targetDomains <- value.targetDomains.traverse(
        DomainId.fromProtoPrimitive(_, "target_domains")
      )
    } yield DomainTrustCertificateX(
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

final case class ParticipantDomainPermissionX(
    domainId: DomainId,
    participantId: ParticipantId,
    permission: ParticipantPermission,
    limits: Option[ParticipantDomainLimits],
    loginAfter: Option[CantonTimestamp],
) extends TopologyMappingX {

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

  override def code: Code = Code.ParticipantDomainPermissionX

  override def namespace: Namespace = participantId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX =
    RequiredUids(Set(domainId.uid))

  override def uniqueKey: MappingHash =
    ParticipantDomainPermissionX.uniqueKey(domainId, participantId)

  def setDefaultLimitIfNotSet(
      defaultLimits: ParticipantDomainLimits
  ): ParticipantDomainPermissionX =
    if (limits.nonEmpty)
      this
    else
      ParticipantDomainPermissionX(
        domainId,
        participantId,
        permission,
        Some(defaultLimits),
        loginAfter,
      )
}

object ParticipantDomainPermissionX {

  def uniqueKey(domainId: DomainId, participantId: ParticipantId): MappingHash =
    TopologyMappingX.buildUniqueKey(
      code
    )(_.add(domainId.toProtoPrimitive).add(participantId.toProtoPrimitive))

  def code: Code = Code.ParticipantDomainPermissionX

  def default(
      domainId: DomainId,
      participantId: ParticipantId,
  ): ParticipantDomainPermissionX =
    ParticipantDomainPermissionX(
      domainId,
      participantId,
      ParticipantPermission.Submission,
      None,
      None,
    )

  def fromProtoV30(
      value: v30.ParticipantDomainPermission
  ): ParsingResult[ParticipantDomainPermissionX] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      permission <- ParticipantPermission.fromProtoV30(value.permission)
      limits = value.limits.map(ParticipantDomainLimits.fromProtoV30)
      loginAfter <- value.loginAfter.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield ParticipantDomainPermissionX(
      domainId,
      participantId,
      permission,
      limits,
      loginAfter,
    )
}

// Party hosting limits
final case class PartyHostingLimitsX(
    domainId: DomainId,
    partyId: PartyId,
    quota: Int,
) extends TopologyMappingX {

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

  override def code: Code = Code.PartyHostingLimitsX

  override def namespace: Namespace = domainId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domainId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX =
    RequiredUids(Set(domainId.uid))

  override def uniqueKey: MappingHash = PartyHostingLimitsX.uniqueKey(domainId, partyId)
}

object PartyHostingLimitsX {

  def uniqueKey(domainId: DomainId, partyId: PartyId): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(
      _.add(domainId.toProtoPrimitive).add(partyId.toProtoPrimitive)
    )

  def code: Code = Code.PartyHostingLimitsX

  def fromProtoV30(
      value: v30.PartyHostingLimits
  ): ParsingResult[PartyHostingLimitsX] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      quota = value.quota
    } yield PartyHostingLimitsX(domainId, partyId, quota)
}

// Package vetting
final case class VettedPackagesX(
    participantId: ParticipantId,
    domainId: Option[DomainId],
    packageIds: Seq[LfPackageId],
) extends TopologyMappingX {

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

  override def code: Code = Code.VettedPackagesX

  override def namespace: Namespace = participantId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToDomain: Option[DomainId] = domainId

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX =
    RequiredUids(Set(participantId.uid))

  override def uniqueKey: MappingHash = VettedPackagesX.uniqueKey(participantId, domainId)
}

object VettedPackagesX {

  def uniqueKey(participantId: ParticipantId, domainId: Option[DomainId]): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(
      _.add(participantId.toProtoPrimitive).add(domainId.fold("")(_.toProtoPrimitive))
    )

  def code: Code = Code.VettedPackagesX

  def fromProtoV30(
      value: v30.VettedPackages
  ): ParsingResult[VettedPackagesX] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      packageIds <- value.packageIds
        .traverse(LfPackageId.fromString)
        .leftMap(ProtoDeserializationError.ValueConversionError("package_ids", _))
      domainId <-
        if (value.domain.nonEmpty)
          DomainId.fromProtoPrimitive(value.domain, "domain").map(_.some)
        else Right(None)
    } yield VettedPackagesX(participantId, domainId, packageIds)
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

final case class PartyToParticipantX(
    partyId: PartyId,
    domainId: Option[DomainId],
    threshold: PositiveInt,
    participants: Seq[HostingParticipant],
    groupAddressing: Boolean,
) extends TopologyMappingX {

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

  override def code: Code = Code.PartyToParticipantX

  override def namespace: Namespace = partyId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToDomain: Option[DomainId] = domainId

  def participantIds: Seq[ParticipantId] = participants.map(_.participantId)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = {
    previous
      .collect {
        case TopologyTransactionX(
              TopologyChangeOpX.Replace,
              _,
              PartyToParticipantX(_, _, prevThreshold, prevParticipants, prevGroupAddressing),
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

  override def uniqueKey: MappingHash = PartyToParticipantX.uniqueKey(partyId, domainId)
}

object PartyToParticipantX {

  def uniqueKey(partyId: PartyId, domainId: Option[DomainId]): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(
      _.add(partyId.toProtoPrimitive).add(domainId.fold("")(_.toProtoPrimitive))
    )

  def code: Code = Code.PartyToParticipantX

  def fromProtoV30(
      value: v30.PartyToParticipant
  ): ParsingResult[PartyToParticipantX] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt(value.threshold)
      participants <- value.participants.traverse(HostingParticipant.fromProtoV30)
      groupAddressing = value.groupAddressing
      domainId <-
        if (value.domain.nonEmpty)
          DomainId.fromProtoPrimitive(value.domain, "domain").map(_.some)
        else Right(None)
    } yield PartyToParticipantX(partyId, domainId, threshold, participants, groupAddressing)
}

// AuthorityOfX
final case class AuthorityOfX(
    partyId: PartyId,
    domainId: Option[DomainId],
    threshold: PositiveInt,
    parties: Seq[PartyId],
) extends TopologyMappingX {

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

  override def code: Code = Code.AuthorityOfX

  override def namespace: Namespace = partyId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToDomain: Option[DomainId] = domainId

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = {
    // TODO(#12390): take the previous transaction into account
    RequiredUids(Set(partyId.uid) ++ parties.map(_.uid))
  }

  override def uniqueKey: MappingHash = AuthorityOfX.uniqueKey(partyId, domainId)
}

object AuthorityOfX {

  def uniqueKey(partyId: PartyId, domainId: Option[DomainId]): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(
      _.add(partyId.toProtoPrimitive).add(domainId.fold("")(_.toProtoPrimitive))
    )

  def code: Code = Code.AuthorityOfX

  def fromProtoV30(
      value: v30.AuthorityOf
  ): ParsingResult[AuthorityOfX] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt(value.threshold)
      parties <- value.parties.traverse(PartyId.fromProtoPrimitive(_, "parties"))
      domainId <-
        if (value.domain.nonEmpty)
          DomainId.fromProtoPrimitive(value.domain, "domain").map(_.some)
        else Right(None)
    } yield AuthorityOfX(partyId, domainId, threshold, parties)
}

/** Dynamic domain parameter settings for the domain
  *
  * Each domain has a set of parameters that can be changed at runtime.
  * These changes are authorized by the owner of the domain and distributed
  * to all nodes accordingly.
  */
final case class DomainParametersStateX(domain: DomainId, parameters: DynamicDomainParameters)
    extends TopologyMappingX {

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.DomainParametersState(
        v30.DomainParametersState(
          domain = domain.toProtoPrimitive,
          domainParameters = Some(parameters.toProtoV30),
        )
      )
    )

  def code: TopologyMappingX.Code = Code.DomainParametersStateX

  override def namespace: Namespace = domain.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = DomainParametersStateX.uniqueKey(domain)
}

object DomainParametersStateX {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMappingX.Code = Code.DomainParametersStateX

  def fromProtoV30(
      value: v30.DomainParametersState
  ): ParsingResult[DomainParametersStateX] = {
    val v30.DomainParametersState(domainIdP, domainParametersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      parameters <- ProtoConverter.parseRequired(
        DynamicDomainParameters.fromProtoV30,
        "domainParameters",
        domainParametersP,
      )
    } yield DomainParametersStateX(domainId, parameters)
  }
}

/** Mediator definition for a domain
  *
  * Each domain needs at least one mediator (group), but can have multiple.
  * Mediators can be temporarily be turned off by making them observers. This way,
  * they get informed but they don't have to reply.
  */
final case class MediatorDomainStateX private (
    domain: DomainId,
    group: NonNegativeInt,
    threshold: PositiveInt,
    active: NonEmpty[Seq[MediatorId]],
    observers: Seq[MediatorId],
) extends TopologyMappingX {

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

  override def code: TopologyMappingX.Code = Code.MediatorDomainStateX

  override def namespace: Namespace = domain.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = MediatorDomainStateX.uniqueKey(domain, group)
}

object MediatorDomainStateX {

  def uniqueKey(domainId: DomainId, group: NonNegativeInt): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive).add(group.unwrap))

  def code: TopologyMappingX.Code = Code.MediatorDomainStateX

  def create(
      domain: DomainId,
      group: NonNegativeInt,
      threshold: PositiveInt,
      active: Seq[MediatorId],
      observers: Seq[MediatorId],
  ): Either[String, MediatorDomainStateX] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of mediator domain state higher than number of mediators ${active.length}",
    )
    activeNE <- NonEmpty
      .from(active)
      .toRight("mediator domain state requires at least one active mediator")
  } yield MediatorDomainStateX(domain, group, threshold, activeNE, observers)

  def fromProtoV30(
      value: v30.MediatorDomainState
  ): ParsingResult[MediatorDomainStateX] = {
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
final case class SequencerDomainStateX private (
    domain: DomainId,
    threshold: PositiveInt,
    active: NonEmpty[Seq[SequencerId]],
    observers: Seq[SequencerId],
) extends TopologyMappingX {

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

  def code: TopologyMappingX.Code = Code.SequencerDomainStateX

  override def namespace: Namespace = domain.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = SequencerDomainStateX.uniqueKey(domain)
}

object SequencerDomainStateX {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMappingX.Code = Code.SequencerDomainStateX

  def create(
      domain: DomainId,
      threshold: PositiveInt,
      active: Seq[SequencerId],
      observers: Seq[SequencerId],
  ): Either[String, SequencerDomainStateX] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of sequencer domain state higher than number of active sequencers ${active.length}",
    )
    activeNE <- NonEmpty
      .from(active)
      .toRight("sequencer domain state requires at least one active sequencer")
  } yield SequencerDomainStateX(domain, threshold, activeNE, observers)

  def fromProtoV30(
      value: v30.SequencerDomainState
  ): ParsingResult[SequencerDomainStateX] = {
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
final case class PurgeTopologyTransactionX private (
    domain: DomainId,
    mappings: NonEmpty[Seq[TopologyMappingX]],
) extends TopologyMappingX {

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

  def code: TopologyMappingX.Code = Code.PurgeTopologyTransactionX

  override def namespace: Namespace = domain.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domain.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(domain.uid))

  override def uniqueKey: MappingHash = PurgeTopologyTransactionX.uniqueKey(domain)
}

object PurgeTopologyTransactionX {

  def uniqueKey(domainId: DomainId): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(_.add(domainId.toProtoPrimitive))

  def code: TopologyMappingX.Code = Code.PurgeTopologyTransactionX

  def create(
      domain: DomainId,
      mappings: Seq[TopologyMappingX],
  ): Either[String, PurgeTopologyTransactionX] = for {
    mappingsToPurge <- NonEmpty
      .from(mappings)
      .toRight("purge topology transaction-x requires at least one topology mapping")
  } yield PurgeTopologyTransactionX(domain, mappingsToPurge)

  def fromProtoV30(
      value: v30.PurgeTopologyTransaction
  ): ParsingResult[PurgeTopologyTransactionX] = {
    val v30.PurgeTopologyTransaction(domainIdP, mappingsP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      mappings <- mappingsP.traverse(TopologyMappingX.fromProtoV30)
      result <- create(domainId, mappings).leftMap(
        ProtoDeserializationError.OtherError
      )
    } yield result
  }

}

// Traffic control state topology transactions
final case class TrafficControlStateX private (
    domain: DomainId,
    member: Member,
    totalExtraTrafficLimit: PositiveLong,
) extends TopologyMappingX {

  def toProto: v30.TrafficControlState = {
    v30.TrafficControlState(
      domain = domain.toProtoPrimitive,
      member = member.toProtoPrimitive,
      totalExtraTrafficLimit = totalExtraTrafficLimit.value,
    )
  }

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.TrafficControlState(
        toProto
      )
    )

  def code: TopologyMappingX.Code = Code.TrafficControlStateX

  override def namespace: Namespace = member.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(member.uid)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX = RequiredUids(Set(domain.uid))

  override def restrictedToDomain: Option[DomainId] = Some(domain)

  override def uniqueKey: MappingHash = TrafficControlStateX.uniqueKey(domain, member)
}

object TrafficControlStateX {

  def uniqueKey(domainId: DomainId, member: Member): MappingHash =
    TopologyMappingX.buildUniqueKey(code)(
      _.add(domainId.toProtoPrimitive).add(member.uid.toProtoPrimitive)
    )

  def code: TopologyMappingX.Code = Code.TrafficControlStateX

  def create(
      domain: DomainId,
      member: Member,
      totalExtraTrafficLimit: PositiveLong,
  ): Either[String, TrafficControlStateX] =
    Right(TrafficControlStateX(domain, member, totalExtraTrafficLimit))

  def fromProtoV30(
      value: v30.TrafficControlState
  ): ParsingResult[TrafficControlStateX] = {
    val v30.TrafficControlState(domainIdP, memberP, totalExtraTrafficLimitP) =
      value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      member <- Member.fromProtoPrimitive(memberP, "member")
      totalExtraTrafficLimit <- PositiveLong
        .create(totalExtraTrafficLimitP)
        .leftMap(e => InvariantViolation(e.message))
      result <- create(domainId, member, totalExtraTrafficLimit).leftMap(
        ProtoDeserializationError.OtherError
      )
    } yield result
  }
}
