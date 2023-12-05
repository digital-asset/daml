// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.protocol.v2.TopologyMappingX.Mapping
import com.digitalasset.canton.protocol.{DynamicDomainParameters, v2}
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

import scala.reflect.ClassTag

sealed trait TopologyMappingX extends Product with Serializable with PrettyPrinting {

  override def pretty: Pretty[this.type] = adHocPrettyInstance

  /** Returns the code used to store & index this mapping */
  def code: Code

  /** The "primary" namespace authorizing the topology mapping.
    * Used for filtering query results.
    */
  def namespace: Namespace

  /** The "primary" identity authorizing the topology mapping, optional as some mappings (namespace delegations and
    * unionspace definitions) only have a namespace
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

  def toProtoV2: v2.TopologyMappingX

  lazy val uniqueKey: MappingHash = {
    // TODO(#14048) use different hash purpose (this one isn't used anymore)
    MappingHash(
      addUniqueKeyToBuilder(
        Hash.build(HashPurpose.DomainTopologyTransactionMessageSignature, HashAlgorithm.Sha256)
      ).add(code.dbInt)
        .finish()
    )
  }

  final def select[TargetMapping <: TopologyMappingX](implicit
      M: ClassTag[TargetMapping]
  ): Option[TargetMapping] = M.unapply(this)

  /** Returns a hash builder based on the values of the topology mapping that needs to be unique */
  protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder

}

object TopologyMappingX {

  final case class MappingHash(hash: Hash) extends AnyVal

  sealed case class Code private (dbInt: Int, code: String)
  object Code {

    object NamespaceDelegationX extends Code(1, "nsd")
    object IdentifierDelegationX extends Code(2, "idd")
    object UnionspaceDefinitionX extends Code(3, "usd")

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

    lazy val all = Seq(
      NamespaceDelegationX,
      IdentifierDelegationX,
      UnionspaceDefinitionX,
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
  ) {
    def isEmpty: Boolean = namespacesWithRoot.isEmpty && namespaces.isEmpty && uids.isEmpty
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

  sealed trait RequiredAuthX {
    def requireRootDelegation: Boolean = false
    def satisfiedByActualAuthorizers(
        namespacesWithRoot: Set[Namespace],
        namespaces: Set[Namespace],
        uids: Set[UniqueIdentifier],
    ): Either[RequiredAuthXAuthorizations, Unit]

    final def and(next: RequiredAuthX): RequiredAuthX =
      RequiredAuthX.And(this, next)
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
        case And(first, second) => T.combine(loop(first), loop(second))
        case Or(first, second) =>
          val firstRes = loop(first)
          if (firstRes == T.empty) loop(second)
          else firstRes
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
    }

    private[transaction] final case class And(
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
          .flatMap(_ =>
            second
              .satisfiedByActualAuthorizers(namespacesWithRoot, namespaces, uids)
          )

      override def authorizations: RequiredAuthXAuthorizations =
        RequiredAuthXAuthorizations.monoid.combine(first.authorizations, second.authorizations)
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
    }
  }

  def fromProtoV2(proto: v2.TopologyMappingX): ParsingResult[TopologyMappingX] =
    proto.mapping match {
      case Mapping.Empty =>
        Left(ProtoDeserializationError.TransactionDeserialization("No mapping set"))
      case Mapping.NamespaceDelegation(value) => NamespaceDelegationX.fromProtoV2(value)
      case Mapping.IdentifierDelegation(value) => IdentifierDelegationX.fromProtoV2(value)
      case Mapping.UnionspaceDefinition(value) => UnionspaceDefinitionX.fromProtoV2(value)
      case Mapping.OwnerToKeyMapping(value) => OwnerToKeyMappingX.fromProtoV2(value)
      case Mapping.DomainTrustCertificate(value) => DomainTrustCertificateX.fromProtoV2(value)
      case Mapping.PartyHostingLimits(value) => PartyHostingLimitsX.fromProtoV2(value)
      case Mapping.ParticipantPermission(value) => ParticipantDomainPermissionX.fromProtoV2(value)
      case Mapping.VettedPackages(value) => VettedPackagesX.fromProtoV2(value)
      case Mapping.PartyToParticipant(value) => PartyToParticipantX.fromProtoV2(value)
      case Mapping.AuthorityOf(value) => AuthorityOfX.fromProtoV2(value)
      case Mapping.DomainParametersState(value) => DomainParametersStateX.fromProtoV2(value)
      case Mapping.MediatorDomainState(value) => MediatorDomainStateX.fromProtoV2(value)
      case Mapping.SequencerDomainState(value) => SequencerDomainStateX.fromProtoV2(value)
      case Mapping.PurgeTopologyTxs(value) => PurgeTopologyTransactionX.fromProtoV2(value)
      case Mapping.TrafficControlState(value) => TrafficControlStateX.fromProtoV2(value)
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

  def toProto: v2.NamespaceDelegationX =
    v2.NamespaceDelegationX(
      namespace = namespace.fingerprint.unwrap,
      targetKey = Some(target.toProtoV0),
      isRootDelegation = isRootDelegation,
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.NamespaceDelegation(
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(namespace.fingerprint.unwrap)
      .add(target.fingerprint.unwrap)
}

object NamespaceDelegationX {

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
    ((sit.transaction.op == TopologyChangeOpX.Replace && sit.transaction.serial == PositiveInt.one) ||
      (sit.transaction.op == TopologyChangeOpX.Remove && sit.transaction.serial != PositiveInt.one)) &&
    sit.transaction.mapping
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
      sit.transaction.op == TopologyChangeOpX.Replace &&
        sit.transaction.mapping
          .select[transaction.NamespaceDelegationX]
          .exists(ns => ns.isRootDelegation)
    )
  }

  def fromProtoV2(
      value: v2.NamespaceDelegationX
  ): ParsingResult[NamespaceDelegationX] =
    for {
      namespace <- Fingerprint.fromProtoPrimitive(value.namespace).map(Namespace(_))
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV0,
        "target_key",
        value.targetKey,
      )
    } yield NamespaceDelegationX(namespace, target, value.isRootDelegation)

}

/** which sequencers are active on the given domain
  *
  * authorization: whoever controls the domain and all the owners of the active or observing sequencers that
  *   were not already present in the tx with serial = n - 1
  *   exception: a sequencer can leave the consortium unilaterally as long as there are enough members
  *              to reach the threshold
  */
final case class UnionspaceDefinitionX private (
    unionspace: Namespace,
    threshold: PositiveInt,
    owners: NonEmpty[Set[Namespace]],
) extends TopologyMappingX {

  def toProto: v2.UnionspaceDefinitionX =
    v2.UnionspaceDefinitionX(
      unionspace = unionspace.fingerprint.unwrap,
      threshold = threshold.unwrap,
      owners = owners.toSeq.map(_.toProtoPrimitive),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.UnionspaceDefinition(
        toProto
      )
    )

  override def code: Code = Code.UnionspaceDefinitionX

  override def namespace: Namespace = unionspace
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
              UnionspaceDefinitionX(`unionspace`, previousThreshold, previousOwners),
            )
          ) =>
        val added = owners.diff(previousOwners)
        // all added owners MUST sign
        RequiredNamespaces(added)
          // and the quorum of existing owners
          .and(
            RequiredNamespaces(
              Set(unionspace)
            )
          )
      case Some(topoTx) =>
        // TODO(#14048): proper error or ignore
        sys.error(s"unexpected transaction data: $previous")
    }
  }

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder.add(unionspace.fingerprint.unwrap)
}

object UnionspaceDefinitionX {

  def code: TopologyMappingX.Code = Code.UnionspaceDefinitionX

  def create(
      unionspace: Namespace,
      threshold: PositiveInt,
      owners: NonEmpty[Set[Namespace]],
  ): Either[String, UnionspaceDefinitionX] =
    for {
      _ <- Either.cond(
        owners.size >= threshold.value,
        (),
        s"Invalid threshold (${threshold}) for ${unionspace} with ${owners.size} owners",
      )
    } yield UnionspaceDefinitionX(unionspace, threshold, owners)

  def fromProtoV2(
      value: v2.UnionspaceDefinitionX
  ): ParsingResult[UnionspaceDefinitionX] = {
    val v2.UnionspaceDefinitionX(unionspaceP, thresholdP, ownersP) = value
    for {
      unionspace <- Fingerprint.fromProtoPrimitive(unionspaceP).map(Namespace(_))
      threshold <- ProtoConverter.parsePositiveInt(thresholdP)
      owners <- ownersP.traverse(Fingerprint.fromProtoPrimitive)
      ownersNE <- NonEmpty
        .from(owners.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            "owners cannot be empty"
          )
        )
      item <- create(unionspace, threshold, ownersNE.map(Namespace(_)))
        .leftMap(ProtoDeserializationError.OtherError)
    } yield item
  }

  def computeNamespace(
      owners: Set[Namespace]
  ): Namespace = {
    val builder = Hash.build(HashPurpose.UnionspaceNamespace, HashAlgorithm.Sha256)
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

  def toProto: v2.IdentifierDelegationX =
    v2.IdentifierDelegationX(
      uniqueIdentifier = identifier.toProtoPrimitive,
      targetKey = Some(target.toProtoV0),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.IdentifierDelegation(
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(identifier.toProtoPrimitive)
      .add(target.fingerprint.unwrap)
}

object IdentifierDelegationX {

  def code: Code = Code.IdentifierDelegationX

  def fromProtoV2(
      value: v2.IdentifierDelegationX
  ): ParsingResult[IdentifierDelegationX] =
    for {
      identifier <- UniqueIdentifier.fromProtoPrimitive(value.uniqueIdentifier, "unique_identifier")
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV0,
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    TopologyMappingX.addDomainId(builder.add(member.uid.toProtoPrimitive), domain)

  def toProto: v2.OwnerToKeyMappingX = v2.OwnerToKeyMappingX(
    member = member.toProtoPrimitive,
    publicKeys = keys.map(_.toProtoPublicKeyV0),
    domain = domain.map(_.toProtoPrimitive).getOrElse(""),
  )

  def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.OwnerToKeyMapping(
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

}

object OwnerToKeyMappingX {

  def code: TopologyMappingX.Code = Code.OwnerToKeyMappingX

  def fromProtoV2(
      value: v2.OwnerToKeyMappingX
  ): ParsingResult[OwnerToKeyMappingX] = {
    val v2.OwnerToKeyMappingX(memberP, keysP, domainP) = value
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      keys <- keysP.traverse(x =>
        ProtoConverter
          .parseRequired(PublicKey.fromProtoPublicKeyV0, "public_keys", Some(x))
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

  def toProto: v2.DomainTrustCertificateX =
    v2.DomainTrustCertificateX(
      participant = participantId.toProtoPrimitive,
      domain = domainId.toProtoPrimitive,
      transferOnlyToGivenTargetDomains = transferOnlyToGivenTargetDomains,
      targetDomains = targetDomains.map(_.toProtoPrimitive),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.DomainTrustCertificate(
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(participantId.toProtoPrimitive)
      .add(domainId.toProtoPrimitive)
}

object DomainTrustCertificateX {

  def code: Code = Code.DomainTrustCertificateX

  def fromProtoV2(
      value: v2.DomainTrustCertificateX
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

/* Participant domain permission
 */
sealed abstract class ParticipantPermissionX(val canConfirm: Boolean)
    extends Product
    with Serializable {
  def toProtoV2: v2.ParticipantPermissionX
  def toNonX: ParticipantPermission
}
object ParticipantPermissionX {
  case object Submission extends ParticipantPermissionX(canConfirm = true) {
    lazy val toProtoV2 = v2.ParticipantPermissionX.Submission
    override def toNonX: ParticipantPermission = ParticipantPermission.Submission
  }
  case object Confirmation extends ParticipantPermissionX(canConfirm = true) {
    lazy val toProtoV2 = v2.ParticipantPermissionX.Confirmation
    override def toNonX: ParticipantPermission = ParticipantPermission.Confirmation
  }
  case object Observation extends ParticipantPermissionX(canConfirm = false) {
    lazy val toProtoV2 = v2.ParticipantPermissionX.Observation
    override def toNonX: ParticipantPermission = ParticipantPermission.Observation
  }

  def fromProtoV2(value: v2.ParticipantPermissionX): ParsingResult[ParticipantPermissionX] =
    value match {
      case v2.ParticipantPermissionX.MissingParticipantPermission =>
        Left(FieldNotSet(value.name))
      case v2.ParticipantPermissionX.Submission => Right(Submission)
      case v2.ParticipantPermissionX.Confirmation => Right(Confirmation)
      case v2.ParticipantPermissionX.Observation => Right(Observation)
      case v2.ParticipantPermissionX.Unrecognized(x) => Left(UnrecognizedEnum(value.name, x))
    }

  implicit val orderingParticipantPermissionX: Ordering[ParticipantPermissionX] = {
    val participantPermissionXOrderMap = Seq[ParticipantPermissionX](
      Observation,
      Confirmation,
      Submission,
    ).zipWithIndex.toMap
    Ordering.by[ParticipantPermissionX, Int](participantPermissionXOrderMap(_))
  }
}

sealed trait TrustLevelX {
  def toProtoV2: v2.TrustLevelX
  def toNonX: TrustLevel
}
object TrustLevelX {
  case object Ordinary extends TrustLevelX {
    lazy val toProtoV2 = v2.TrustLevelX.Ordinary
    def toNonX: TrustLevel = TrustLevel.Ordinary
  }
  case object Vip extends TrustLevelX {
    lazy val toProtoV2 = v2.TrustLevelX.Vip
    def toNonX: TrustLevel = TrustLevel.Vip
  }

  def fromProtoV2(value: v2.TrustLevelX): ParsingResult[TrustLevelX] = value match {
    case v2.TrustLevelX.Ordinary => Right(Ordinary)
    case v2.TrustLevelX.Vip => Right(Vip)
    case v2.TrustLevelX.MissingTrustLevel => Left(FieldNotSet(value.name))
    case v2.TrustLevelX.Unrecognized(x) => Left(UnrecognizedEnum(value.name, x))
  }

  implicit val orderingTrustLevelX: Ordering[TrustLevelX] = {
    val participantTrustLevelXOrderMap =
      Seq[TrustLevelX](Ordinary, Vip).zipWithIndex.toMap
    Ordering.by[TrustLevelX, Int](participantTrustLevelXOrderMap(_))
  }
}

final case class ParticipantDomainLimits(maxRate: Int, maxNumParties: Int, maxNumPackages: Int) {
  def toProto: v2.ParticipantDomainLimits =
    v2.ParticipantDomainLimits(maxRate, maxNumParties, maxNumPackages)
}
object ParticipantDomainLimits {
  def fromProtoV2(value: v2.ParticipantDomainLimits): ParticipantDomainLimits =
    ParticipantDomainLimits(value.maxRate, value.maxNumParties, value.maxNumPackages)
}

final case class ParticipantDomainPermissionX(
    domainId: DomainId,
    participantId: ParticipantId,
    permission: ParticipantPermissionX,
    trustLevel: TrustLevelX,
    limits: Option[ParticipantDomainLimits],
    loginAfter: Option[CantonTimestamp],
) extends TopologyMappingX {

  def toParticipantAttributes: ParticipantAttributes =
    ParticipantAttributes(permission.toNonX, trustLevel.toNonX)

  def toProto: v2.ParticipantDomainPermissionX =
    v2.ParticipantDomainPermissionX(
      domain = domainId.toProtoPrimitive,
      participant = participantId.toProtoPrimitive,
      permission = permission.toProtoV2,
      trustLevel = trustLevel.toProtoV2,
      limits = limits.map(_.toProto),
      loginAfter = loginAfter.map(_.toProtoPrimitive),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.ParticipantPermission(
        toProto
      )
    )

  override def code: Code = Code.ParticipantDomainPermissionX

  override def namespace: Namespace = domainId.uid.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(domainId.uid)

  override def restrictedToDomain: Option[DomainId] = Some(domainId)

  override def requiredAuth(
      previous: Option[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): RequiredAuthX =
    RequiredUids(Set(domainId.uid))

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(domainId.toProtoPrimitive)
      .add(participantId.toProtoPrimitive)

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
        trustLevel,
        Some(defaultLimits),
        loginAfter,
      )
}

object ParticipantDomainPermissionX {

  def code: Code = Code.ParticipantDomainPermissionX

  def default(
      domainId: DomainId,
      participantId: ParticipantId,
  ): ParticipantDomainPermissionX =
    ParticipantDomainPermissionX(
      domainId,
      participantId,
      ParticipantPermissionX.Submission,
      TrustLevelX.Ordinary,
      None,
      None,
    )

  def fromProtoV2(
      value: v2.ParticipantDomainPermissionX
  ): ParsingResult[ParticipantDomainPermissionX] =
    for {
      domainId <- DomainId.fromProtoPrimitive(value.domain, "domain")
      participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
      permission <- ParticipantPermissionX.fromProtoV2(value.permission)
      trustLevel <- TrustLevelX.fromProtoV2(value.trustLevel)
      limits = value.limits.map(ParticipantDomainLimits.fromProtoV2)
      loginAfter <- value.loginAfter.fold[ParsingResult[Option[CantonTimestamp]]](Right(None))(
        CantonTimestamp.fromProtoPrimitive(_).map(_.some)
      )
    } yield ParticipantDomainPermissionX(
      domainId,
      participantId,
      permission,
      trustLevel,
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

  def toProto: v2.PartyHostingLimitsX =
    v2.PartyHostingLimitsX(
      domain = domainId.toProtoPrimitive,
      party = partyId.toProtoPrimitive,
      quota = quota,
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.PartyHostingLimits(
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(domainId.toProtoPrimitive)
      .add(partyId.toProtoPrimitive)
}

object PartyHostingLimitsX {

  def code: Code = Code.PartyHostingLimitsX

  def fromProtoV2(
      value: v2.PartyHostingLimitsX
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

  def toProto: v2.VettedPackagesX =
    v2.VettedPackagesX(
      participant = participantId.toProtoPrimitive,
      packageIds = packageIds,
      domain = domainId.fold("")(_.toProtoPrimitive),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.VettedPackages(
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(participantId.toProtoPrimitive)
      .add(domainId.fold("")(_.toProtoPrimitive))
}

object VettedPackagesX {

  def code: Code = Code.VettedPackagesX

  def fromProtoV2(
      value: v2.VettedPackagesX
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
    permission: ParticipantPermissionX,
) {
  def toProto: v2.PartyToParticipantX.HostingParticipant =
    v2.PartyToParticipantX.HostingParticipant(
      participant = participantId.toProtoPrimitive,
      permission = permission.toProtoV2,
    )
}

object HostingParticipant {
  def fromProtoV2(
      value: v2.PartyToParticipantX.HostingParticipant
  ): ParsingResult[HostingParticipant] = for {
    participantId <- ParticipantId.fromProtoPrimitive(value.participant, "participant")
    permission <- ParticipantPermissionX.fromProtoV2(value.permission)
  } yield HostingParticipant(participantId, permission)
}

final case class PartyToParticipantX(
    partyId: PartyId,
    domainId: Option[DomainId],
    threshold: PositiveInt,
    participants: Seq[HostingParticipant],
    groupAddressing: Boolean,
) extends TopologyMappingX {

  def toProto: v2.PartyToParticipantX =
    v2.PartyToParticipantX(
      party = partyId.toProtoPrimitive,
      threshold = threshold.value,
      participants = participants.map(_.toProto),
      groupAddressing = groupAddressing,
      domain = domainId.fold("")(_.toProtoPrimitive),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.PartyToParticipant(
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
    // TODO(#12390): take into account the previous transaction and allow participants to unilaterally
    //   disassociate themselves from a party as long as the threshold can still be reached
    previous
      .collect {
        case TopologyTransactionX(
              TopologyChangeOpX.Replace,
              _,
              PartyToParticipantX(partyId, _, _, previousParticipants, _),
            ) =>
          val addedParticipants = participants
            .map(_.participantId.uid)
            .diff(previousParticipants.map(_.participantId.uid))
          RequiredUids(
            Set(partyId.uid) ++ addedParticipants
          )
      }
      .getOrElse(
        RequiredUids(Set(partyId.uid) ++ participants.map(_.participantId.uid))
      )
  }

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(partyId.toProtoPrimitive)
      .add(domainId.fold("")(_.toProtoPrimitive))
}

object PartyToParticipantX {

  def code: Code = Code.PartyToParticipantX

  def fromProtoV2(
      value: v2.PartyToParticipantX
  ): ParsingResult[PartyToParticipantX] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt(value.threshold)
      participants <- value.participants.traverse(HostingParticipant.fromProtoV2)
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

  def toProto: v2.AuthorityOfX =
    v2.AuthorityOfX(
      party = partyId.toProtoPrimitive,
      threshold = threshold.unwrap,
      parties = parties.map(_.toProtoPrimitive),
      domain = domainId.fold("")(_.toProtoPrimitive),
    )

  override def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.AuthorityOf(
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder
      .add(partyId.toProtoPrimitive)
      .add(domainId.fold("")(_.toProtoPrimitive))
}

object AuthorityOfX {

  def code: Code = Code.AuthorityOfX

  def fromProtoV2(
      value: v2.AuthorityOfX
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder.add(domain.uid.toProtoPrimitive)

  def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.DomainParametersState(
        v2.DomainParametersStateX(
          domain = domain.toProtoPrimitive,
          domainParameters = Some(parameters.toProtoV2),
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
}

object DomainParametersStateX {

  def code: TopologyMappingX.Code = Code.DomainParametersStateX

  def fromProtoV2(
      value: v2.DomainParametersStateX
  ): ParsingResult[DomainParametersStateX] = {
    val v2.DomainParametersStateX(domainIdP, domainParametersP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      parameters <- ProtoConverter.parseRequired(
        DynamicDomainParameters.fromProtoV2,
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

  lazy val allMediatorsInGroup = active ++ observers

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder.add(domain.uid.toProtoPrimitive).add(group.unwrap)

  def toProto: v2.MediatorDomainStateX =
    v2.MediatorDomainStateX(
      domain = domain.toProtoPrimitive,
      group = group.unwrap,
      threshold = threshold.unwrap,
      active = active.map(_.uid.toProtoPrimitive),
      observers = observers.map(_.uid.toProtoPrimitive),
    )

  def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.MediatorDomainState(
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
}

object MediatorDomainStateX {

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
      s"threshold (${threshold}) of mediator domain state higher than number of mediators ${active.length}",
    )
    activeNE <- NonEmpty
      .from(active)
      .toRight("mediator domain state requires at least one active mediator")
  } yield MediatorDomainStateX(domain, group, threshold, activeNE, observers)

  def fromProtoV2(
      value: v2.MediatorDomainStateX
  ): ParsingResult[MediatorDomainStateX] = {
    val v2.MediatorDomainStateX(domainIdP, groupP, thresholdP, activeP, observersP) = value
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

  lazy val allSequencers = active ++ observers

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder.add(domain.uid.toProtoPrimitive)

  def toProto: v2.SequencerDomainStateX =
    v2.SequencerDomainStateX(
      domain = domain.toProtoPrimitive,
      threshold = threshold.unwrap,
      active = active.map(_.uid.toProtoPrimitive),
      observers = observers.map(_.uid.toProtoPrimitive),
    )

  def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.SequencerDomainState(
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
}

object SequencerDomainStateX {

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
      s"threshold (${threshold}) of sequencer domain state higher than number of active sequencers ${active.length}",
    )
    activeNE <- NonEmpty
      .from(active)
      .toRight("sequencer domain state requires at least one active sequencer")
  } yield SequencerDomainStateX(domain, threshold, activeNE, observers)

  def fromProtoV2(
      value: v2.SequencerDomainStateX
  ): ParsingResult[SequencerDomainStateX] = {
    val v2.SequencerDomainStateX(domainIdP, thresholdP, activeP, observersP) = value
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder.add(domain.uid.toProtoPrimitive)

  def toProto: v2.PurgeTopologyTransactionX =
    v2.PurgeTopologyTransactionX(
      domain = domain.toProtoPrimitive,
      mappings = mappings.map(_.toProtoV2),
    )

  def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.PurgeTopologyTxs(
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
}

object PurgeTopologyTransactionX {

  def code: TopologyMappingX.Code = Code.PurgeTopologyTransactionX

  def create(
      domain: DomainId,
      mappings: Seq[TopologyMappingX],
  ): Either[String, PurgeTopologyTransactionX] = for {
    mappingsToPurge <- NonEmpty
      .from(mappings)
      .toRight("purge topology transaction-x requires at least one topology mapping")
  } yield PurgeTopologyTransactionX(domain, mappingsToPurge)

  def fromProtoV2(
      value: v2.PurgeTopologyTransactionX
  ): ParsingResult[PurgeTopologyTransactionX] = {
    val v2.PurgeTopologyTransactionX(domainIdP, mappingsP) = value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain")
      mappings <- mappingsP.traverse(TopologyMappingX.fromProtoV2)
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

  override protected def addUniqueKeyToBuilder(builder: HashBuilder): HashBuilder =
    builder.add(domain.uid.toProtoPrimitive).add(member.uid.toProtoPrimitive)

  def toProto: v2.TrafficControlStateX = {
    v2.TrafficControlStateX(
      domain = domain.toProtoPrimitive,
      member = member.toProtoPrimitive,
      totalExtraTrafficLimit = totalExtraTrafficLimit.value,
    )
  }

  def toProtoV2: v2.TopologyMappingX =
    v2.TopologyMappingX(
      v2.TopologyMappingX.Mapping.TrafficControlState(
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
}

object TrafficControlStateX {

  def code: TopologyMappingX.Code = Code.TrafficControlStateX

  def create(
      domain: DomainId,
      member: Member,
      totalExtraTrafficLimit: PositiveLong,
  ): Either[String, TrafficControlStateX] =
    Right(TrafficControlStateX(domain, member, totalExtraTrafficLimit))

  def fromProtoV2(
      value: v2.TrafficControlStateX
  ): ParsingResult[TrafficControlStateX] = {
    val v2.TrafficControlStateX(domainIdP, memberP, totalExtraTrafficLimitP) =
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
