// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.PrettyInstances.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** X -> Y */
sealed trait TopologyMapping extends Product with Serializable with PrettyPrinting {

  override def pretty: Pretty[this.type] = adHocPrettyInstance
  def uniquePath(elementId: TopologyElementId): UniquePath
  def dbType: DomainTopologyTransactionType
  def requiredAuth: RequiredAuth

  /** Secondary uid for cascading namespace updates
    *
    * During topology processing (in 2.x), we need in case of cascading updates to fetch
    * all transactions that might be affected due to a new namespace or identifier mapping.
    *
    * Now, the topology transactions have a primary uid (i.e. the party id) within the unique path
    * which is used to index the data, but can have a secondary uid (the participant). We store them by
    * the primary uid but during cascading updates, we actually have to fetch them using the secondary uid.
    *
    * Only txs with RequestSide can have a secondary uid.
    *
    * In 3.x this is simpler, as we removed cascading additions. So all this logic can be deleted soon again ...
    */
  def secondaryUid: Option[UniqueIdentifier] = None

  /** Returns true if the new mapping would be a replacement for the given mapping */
  def isReplacedBy(mapping: TopologyMapping): Boolean = false

  def restrictedToDomain: Option[DomainId] = None

}

sealed trait TopologyStateUpdateMapping extends TopologyMapping
sealed trait DomainGovernanceMapping extends TopologyMapping {
  def domainId: DomainId

  override def uniquePath(
      _elementId: TopologyElementId
  ): UniquePathSignedDomainGovernanceTransaction =
    UniquePathSignedDomainGovernanceTransaction(domainId.unwrap, dbType)
}

/** A namespace delegation transaction (intermediate CA)
  *
  * Entrusts a public-key to perform changes on the namespace
  * {(*,I) => p_k}
  *
  * If the delegation is a root delegation, then the target key
  * inherits the right to authorize other NamespaceDelegations.
  */
// architecture-handbook-entry-begin: NamespaceDelegation
final case class NamespaceDelegation(
    namespace: Namespace,
    target: SigningPublicKey,
    isRootDelegation: Boolean,
) extends TopologyStateUpdateMapping {
  // architecture-handbook-entry-end: NamespaceDelegation
  // TODO(i12892): Add a private constructor, private apply, and factory method to check constraint
  require(
    isRootDelegation || namespace.fingerprint != target.fingerprint,
    s"Root certificate for $namespace needs to be set as isRootDelegation = true",
  )

  def toProtoV30: v30.NamespaceDelegation =
    v30.NamespaceDelegation(
      namespace = namespace.fingerprint.unwrap,
      targetKey = Some(target.toProtoV30),
      isRootDelegation = isRootDelegation,
    )

  // TODO(i4933) include hash over content
  override def uniquePath(id: TopologyElementId): UniquePath =
    UniquePathNamespaceDelegation(namespace, id)

  override def dbType: DomainTopologyTransactionType = NamespaceDelegation.dbType

  override def requiredAuth: RequiredAuth =
    RequiredAuth.Ns(namespace, rootDelegation = true)

}

object NamespaceDelegation {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.NamespaceDelegation

  /** Returns true if the given transaction is a self-signed root certificate */
  def isRootCertificate(sit: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
    sit.transaction.element.mapping match {
      case nd: NamespaceDelegation =>
        nd.namespace.fingerprint == sit.key.fingerprint && nd.isRootDelegation && nd.target.fingerprint == nd.namespace.fingerprint &&
        sit.operation == TopologyChangeOp.Add
      case _ => false
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

/** An identifier delegation
  *
  * entrusts a public-key to do any change with respect to the identifier
  * {(X,I) => p_k}
  */
// architecture-handbook-entry-begin: IdentifierDelegation
final case class IdentifierDelegation(identifier: UniqueIdentifier, target: SigningPublicKey)
    extends TopologyStateUpdateMapping {
  // architecture-handbook-entry-end: IdentifierDelegation
  def toProtoV30: v30.IdentifierDelegation =
    v30.IdentifierDelegation(
      uniqueIdentifier = identifier.toProtoPrimitive,
      targetKey = Some(target.toProtoV30),
    )

  // TODO(i4933) include hash over content
  override def uniquePath(id: TopologyElementId): UniquePath =
    UniquePathSignedTopologyTransaction(identifier, dbType, id)

  override def dbType: DomainTopologyTransactionType = IdentifierDelegation.dbType

  override def requiredAuth: RequiredAuth = RequiredAuth.Ns(identifier.namespace, false)

}

object IdentifierDelegation {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.IdentifierDelegation

  def fromProtoV30(
      value: v30.IdentifierDelegation
  ): ParsingResult[IdentifierDelegation] =
    for {
      identifier <- UniqueIdentifier.fromProtoPrimitive(value.uniqueIdentifier, "uniqueIdentifier")
      target <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "target_key",
        value.targetKey,
      )
    } yield IdentifierDelegation(identifier, target)
}

/** A key owner (participant, mediator, sequencer, manager) to key mapping
  *
  * In Canton, we need to know keys for all participating entities. The entities are
  * all the protocol members (participant, mediator, topology manager) plus the
  * sequencer (which provides the communication infrastructure for the members).
  */
// architecture-handbook-entry-begin: OwnerToKeyMapping
final case class OwnerToKeyMapping(owner: Member, key: PublicKey)
    extends TopologyStateUpdateMapping {
  // architecture-handbook-entry-end: OwnerToKeyMapping
  def toProtoV30: v30.OwnerToKeyMapping =
    v30.OwnerToKeyMapping(
      keyOwner = owner.toProtoPrimitive,
      publicKey = Some(key.toProtoPublicKeyV30),
    )

  override def uniquePath(id: TopologyElementId): UniquePath =
    // TODO(i4933) include hash over content
    UniquePathSignedTopologyTransaction(owner.uid, dbType, id)

  override def dbType: DomainTopologyTransactionType = OwnerToKeyMapping.dbType
  override def requiredAuth: RequiredAuth = RequiredAuth.Uid(Seq(owner.uid))

}

object OwnerToKeyMapping {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.OwnerToKeyMapping

  def fromProtoV30(
      value: v30.OwnerToKeyMapping
  ): ParsingResult[OwnerToKeyMapping] =
    for {
      owner <- Member.fromProtoPrimitive(value.keyOwner, "keyOwner")
      key <- ProtoConverter
        .parseRequired(PublicKey.fromProtoPublicKeyV30, "public_key", value.publicKey)
    } yield OwnerToKeyMapping(owner, key)

}

// Using private because the `claim` needs to be a `LegalIdentityClaim`
final case class SignedLegalIdentityClaim private (
    uid: UniqueIdentifier,
    claim: ByteString,
    signature: Signature,
) extends TopologyStateUpdateMapping
    with PrettyPrinting {
  def toProtoV30: v30.SignedLegalIdentityClaim =
    v30.SignedLegalIdentityClaim(
      claim = claim,
      signature = signature.toProtoV30.some,
    )

  override def pretty: Pretty[SignedLegalIdentityClaim] =
    prettyOfClass(param("signature", _.signature), paramWithoutValue("claim"))

  override def uniquePath(id: TopologyElementId): UniquePath =
    // TODO(i4933) include hash over content
    UniquePathSignedTopologyTransaction(uid, dbType, id)

  override def dbType: DomainTopologyTransactionType = SignedLegalIdentityClaim.dbType

  override def requiredAuth: RequiredAuth = RequiredAuth.Uid(Seq(uid))

}

object SignedLegalIdentityClaim {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.SignedLegalIdentityClaim

  @VisibleForTesting
  def create(claim: LegalIdentityClaim, signature: Signature): SignedLegalIdentityClaim =
    SignedLegalIdentityClaim(claim.uid, claim.toByteString, signature)

  def fromProtoV30(
      protocolVersionValidation: ProtocolVersionValidation,
      value: v30.SignedLegalIdentityClaim,
  ): ParsingResult[SignedLegalIdentityClaim] =
    for {
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        value.signature,
      )
      claim <- LegalIdentityClaim.fromByteString(protocolVersionValidation)(value.claim)
    } yield SignedLegalIdentityClaim(claim.uid, value.claim, signature)
}

final case class LegalIdentityClaim private (
    uid: UniqueIdentifier,
    evidence: LegalIdentityClaimEvidence,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      LegalIdentityClaim.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends ProtocolVersionedMemoizedEvidence
    with HasProtocolVersionedWrapper[LegalIdentityClaim] {
  @transient override protected lazy val companionObj: LegalIdentityClaim.type = LegalIdentityClaim

  protected def toProtoV30: v30.LegalIdentityClaim =
    v30.LegalIdentityClaim(
      uniqueIdentifier = uid.toProtoPrimitive,
      evidence = evidence.toProtoOneOf,
    )

  def hash(hashOps: HashOps): Hash =
    hashOps.digest(HashPurpose.LegalIdentityClaim, getCryptographicEvidence)

  override protected def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}

object LegalIdentityClaim extends HasMemoizedProtocolVersionedWrapperCompanion[LegalIdentityClaim] {
  override val name: String = "LegalIdentityClaim"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.LegalIdentityClaim)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(
      uid: UniqueIdentifier,
      evidence: LegalIdentityClaimEvidence,
      protocolVersion: ProtocolVersion,
  ): LegalIdentityClaim =
    LegalIdentityClaim(uid, evidence)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV30(
      claimP: v30.LegalIdentityClaim
  )(bytes: ByteString): ParsingResult[LegalIdentityClaim] =
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(claimP.uniqueIdentifier, "uniqueIdentifier")
      evidence <- LegalIdentityClaimEvidence.fromProtoOneOf(claimP.evidence)
    } yield LegalIdentityClaim(uid, evidence)(
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
}

sealed trait LegalIdentityClaimEvidence {
  def toProtoOneOf: v30.LegalIdentityClaim.Evidence
}

object LegalIdentityClaimEvidence {
  final case class X509Cert(pem: X509CertificatePem) extends LegalIdentityClaimEvidence {
    override def toProtoOneOf: v30.LegalIdentityClaim.Evidence.X509Cert =
      v30.LegalIdentityClaim.Evidence.X509Cert(pem.unwrap)
  }

  def fromProtoOneOf(
      evidenceP: v30.LegalIdentityClaim.Evidence
  ): ParsingResult[LegalIdentityClaimEvidence] = {
    evidenceP match {
      case v30.LegalIdentityClaim.Evidence.X509Cert(pem) =>
        X509CertificatePem
          .fromBytes(pem)
          .map(X509Cert)
          .leftMap(err => ProtoDeserializationError.OtherError(s"Failed to parse PEM: $err"))
      case v30.LegalIdentityClaim.Evidence.Empty =>
        FieldNotSet("LegalIdentityClaim.evidence").asLeft
    }
  }

}

/** Side of the party to participant mapping request
  *
  * Party to participant mapping request need to be approved by both namespaces if the namespaces are different.
  * We support this by allowing to split the signatures into two transactions (so both namespace controller sign the
  * same transaction, just with different "RequestSide"
  *
  * {Both, +, (P,I) -> (N,J)}^[s_I, s_J] = {From,+, (P,I) -> (N,J)}^[s_I] + {To,+, (P,I) -> (N,J)}&#94;[s_J]
  */
sealed trait RequestSide {

  def toProtoEnum: v30.RequestSide

  def requiredAuth(left: UniqueIdentifier, right: UniqueIdentifier): RequiredAuth

}

object RequestSide {

  case object From extends RequestSide {
    val toProtoEnum = v30.RequestSide.From
    override def requiredAuth(left: UniqueIdentifier, right: UniqueIdentifier): RequiredAuth =
      RequiredAuth.Uid(Seq(left))
  }
  case object To extends RequestSide {
    val toProtoEnum = v30.RequestSide.To
    override def requiredAuth(left: UniqueIdentifier, right: UniqueIdentifier): RequiredAuth =
      RequiredAuth.Uid(Seq(right))
  }
  case object Both extends RequestSide {
    val toProtoEnum = v30.RequestSide.Both
    override def requiredAuth(left: UniqueIdentifier, right: UniqueIdentifier): RequiredAuth =
      RequiredAuth.Uid(Seq(left, right))
  }

  /* flips the request side (From becomes To and To becomes From). If Both is passed, an exception is thrown. */
  def flip(side: RequestSide): RequestSide = side match {
    case From => To
    case To => From
    case Both =>
      throw new IllegalArgumentException("should never flip request side of type " + Both.toString)
  }

  def fromProtoEnum(side: v30.RequestSide): ParsingResult[RequestSide] =
    side match {
      case v30.RequestSide.Both => Right(RequestSide.Both)
      case v30.RequestSide.From => Right(RequestSide.From)
      case v30.RequestSide.To => Right(RequestSide.To)
      case v30.RequestSide.MissingRequestSide => Left(FieldNotSet(side.name))
      case v30.RequestSide.Unrecognized(x) => Left(UnrecognizedEnum(side.name, x))
    }

  /** sides accumulator, used in folds in order to figure out if we've seen both sides */
  def accumulateSide(cur: (Boolean, Boolean), side: RequestSide): (Boolean, Boolean) =
    (cur, side) match {
      case (_, RequestSide.Both) => (true, true)
      case ((_, rght), RequestSide.From) => (true, rght)
      case ((lft, _), RequestSide.To) => (lft, true)
    }

}

// architecture-handbook-entry-begin: ParticipantState
final case class ParticipantState(
    side: RequestSide,
    domain: DomainId,
    participant: ParticipantId,
    permission: ParticipantPermission,
    trustLevel: TrustLevel,
) extends TopologyStateUpdateMapping {

  require(
    permission.canConfirm || trustLevel == TrustLevel.Ordinary,
    "participant trust level must either be ordinary or permission must be confirming",
  )
  // architecture-handbook-entry-end: ParticipantState

  def toParticipantAttributes: ParticipantAttributes =
    ParticipantAttributes(permission, trustLevel, None)

  def toProtoV30: v30.ParticipantState = {
    v30.ParticipantState(
      side = side.toProtoEnum,
      domain = domain.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      permission = permission.toProtoEnum,
      trustLevel = trustLevel.toProtoEnum,
    )
  }

  override def uniquePath(id: TopologyElementId): UniquePath = {
    // TODO(i4933) include hash over content and include domain-id in the path
    UniquePathSignedTopologyTransaction(participant.uid, dbType, id)
  }

  override def dbType: DomainTopologyTransactionType = ParticipantState.dbType
  override def requiredAuth: RequiredAuth = side.requiredAuth(domain.unwrap, participant.uid)

  override def secondaryUid: Option[UniqueIdentifier] =
    if (side != RequestSide.To) domain.uid.some else None

  override def isReplacedBy(mapping: TopologyMapping): Boolean = mapping match {
    case other: ParticipantState =>
      def subset(mp: ParticipantState) = (mp.side, mp.domain, mp.participant)
      subset(other) == subset(this)
    case _ => false
  }

  override def restrictedToDomain: Option[DomainId] = Some(domain)

}

object ParticipantState {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.ParticipantState

  def fromProtoV30(
      parsed: v30.ParticipantState
  ): ParsingResult[ParticipantState] =
    for {
      side <- RequestSide.fromProtoEnum(parsed.side)
      domain <- DomainId.fromProtoPrimitive(parsed.domain, "domain")
      permission <- ParticipantPermission.fromProtoEnum(parsed.permission)
      trustLevel <- TrustLevel.fromProtoEnum(parsed.trustLevel)
      uid <- UniqueIdentifier.fromProtoPrimitive(parsed.participant, "participant")
    } yield ParticipantState(side, domain, ParticipantId(uid), permission, trustLevel)

}

// architecture-handbook-entry-begin: MediatorDomainState
final case class MediatorDomainState(
    side: RequestSide,
    domain: DomainId,
    mediator: MediatorId,
) extends TopologyStateUpdateMapping {

  // architecture-handbook-entry-end: MediatorDomainState

  def toProtoV30: v30.MediatorDomainState = {
    v30.MediatorDomainState(
      side = side.toProtoEnum,
      domain = domain.toProtoPrimitive,
      mediator = mediator.uid.toProtoPrimitive,
    )
  }

  override def uniquePath(id: TopologyElementId): UniquePath = {
    // TODO(i4933) include hash over content and include domain-id in the path
    UniquePathSignedTopologyTransaction(mediator.uid, dbType, id)
  }

  override def secondaryUid: Option[UniqueIdentifier] =
    if (side != RequestSide.From) mediator.uid.some else None

  override def dbType: DomainTopologyTransactionType = MediatorDomainState.dbType
  override def requiredAuth: RequiredAuth = side.requiredAuth(domain.unwrap, mediator.uid)

  override def isReplacedBy(mapping: TopologyMapping): Boolean = mapping match {
    case other: MediatorDomainState =>
      def subset(mp: MediatorDomainState) = (mp.side, mp.domain, mp.mediator)
      subset(other) == subset(this)
    case _ => false
  }

  override def restrictedToDomain: Option[DomainId] = Some(domain)

}

object MediatorDomainState {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.MediatorDomainState

  def fromProtoV30(
      parsed: v30.MediatorDomainState
  ): ParsingResult[MediatorDomainState] =
    for {
      side <- RequestSide.fromProtoEnum(parsed.side)
      domain <- DomainId.fromProtoPrimitive(parsed.domain, "domain")
      uid <- UniqueIdentifier.fromProtoPrimitive(parsed.mediator, "mediator")
    } yield MediatorDomainState(side, domain, MediatorId(uid))

}

/** party to participant mapping
  *
  * We can map a party to several participants at the same time. We represent such a
  * mapping in the topology state using the party to participant
  */
// architecture-handbook-entry-begin: PartyToParticipant
final case class PartyToParticipant(
    side: RequestSide,
    party: PartyId,
    participant: ParticipantId,
    permission: ParticipantPermission,
) extends TopologyStateUpdateMapping {
  // architecture-handbook-entry-end: PartyToParticipant

  require(
    party.uid != participant.uid,
    s"Unable to allocate party ${party.uid}, as it has the same name as the participant's admin party.",
  )

  def toProtoV30: v30.PartyToParticipant =
    v30.PartyToParticipant(
      side = side.toProtoEnum,
      party = party.toProtoPrimitive,
      participant = participant.toProtoPrimitive,
      permission = permission.toProtoEnum,
    )

  override def uniquePath(id: TopologyElementId): UniquePath =
    // TODO(i4933) include hash over content
    UniquePathSignedTopologyTransaction(party.uid, dbType, id)

  override def dbType: DomainTopologyTransactionType = PartyToParticipant.dbType

  override def requiredAuth: RequiredAuth = side.requiredAuth(party.uid, participant.uid)

  override def secondaryUid: Option[UniqueIdentifier] =
    if (side != RequestSide.From) participant.uid.some else None

  override def isReplacedBy(mapping: TopologyMapping): Boolean = mapping match {
    case other: PartyToParticipant =>
      def subset(mp: PartyToParticipant) = (mp.side, mp.party, mp.participant)
      subset(other) == subset(this)
    case _ => false
  }

}

object PartyToParticipant {

  def dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.PartyToParticipant

  def fromProtoV30(
      value: v30.PartyToParticipant
  ): ParsingResult[PartyToParticipant] = {
    val v30.PartyToParticipant(sideP, partyP, participantP, permissionP) = value
    for {
      partyUid <- UniqueIdentifier.fromProtoPrimitive(partyP, "party")
      participant <- ParticipantId.fromProtoPrimitive(participantP, "participant")
      side <- RequestSide.fromProtoEnum(sideP)
      permission <- ParticipantPermission.fromProtoEnum(permissionP)
    } yield PartyToParticipant(side, PartyId(partyUid), participant, permission)
  }

}

final case class VettedPackages(participant: ParticipantId, packageIds: Seq[LfPackageId])
    extends TopologyStateUpdateMapping
    with PrettyPrinting {
  def toProtoV30: v30.VettedPackages =
    v30.VettedPackages(
      participant =
        participant.uid.toProtoPrimitive, // use UID proto, not participant proto (as this would be Member.toProtoPrimitive) which includes the unnecessary code
      packageIds = packageIds,
    )

  override def pretty: Pretty[VettedPackages] =
    prettyOfClass(param("participant", _.participant.uid), param("packages", _.packageIds))

  override def uniquePath(id: TopologyElementId): UniquePath =
    // TODO(i4933) include hash over content
    UniquePathSignedTopologyTransaction(participant.uid, dbType, id)

  override def dbType: DomainTopologyTransactionType = VettedPackages.dbType

  override def requiredAuth: RequiredAuth = RequiredAuth.Uid(Seq(participant.uid))

}

object VettedPackages {
  val dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.PackageUse

  def fromProtoV30(value: v30.VettedPackages): ParsingResult[VettedPackages] = {
    val v30.VettedPackages(participantP, packagesP) = value
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(participantP, "participant")
      packageIds <- packagesP
        .traverse(LfPackageId.fromString)
        .leftMap(ProtoDeserializationError.ValueConversionError("package_ids", _))
    } yield VettedPackages(ParticipantId(uid), packageIds)
  }

}

final case class DomainParametersChange(
    domainId: DomainId,
    domainParameters: DynamicDomainParameters,
) extends DomainGovernanceMapping {
  private[transaction] def toProtoV30: v30.DomainParametersChange = v30.DomainParametersChange(
    domain = domainId.toProtoPrimitive,
    Option(domainParameters.toProtoV30),
  )

  override def dbType: DomainTopologyTransactionType = DomainParametersChange.dbType

  override def requiredAuth: RequiredAuth = RequiredAuth.Uid(Seq(domainId.unwrap))
}

object DomainParametersChange {
  val dbType: DomainTopologyTransactionType = DomainTopologyTransactionType.DomainParameters

  private[transaction] def fromProtoV1(
      value: v30.DomainParametersChange
  ): ParsingResult[DomainParametersChange] = {
    for {
      uid <- UniqueIdentifier.fromProtoPrimitive(value.domain, "domain")
      domainParametersXP <- value.domainParameters.toRight(FieldNotSet("domainParameters"))
      domainParameters <- DynamicDomainParameters.fromProtoV30(domainParametersXP)
    } yield DomainParametersChange(DomainId(uid), domainParameters)
  }
}
