// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.Monoid
import cats.instances.order.*
import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  UnrecognizedEnum,
  ValueConversionError,
  ValueDeserializationError,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.{Endpoint, UrlValidator}
import com.digitalasset.canton.protocol.v30.Enums
import com.digitalasset.canton.protocol.v30.Enums.ParticipantFeatureFlag
import com.digitalasset.canton.protocol.v30.NamespaceDelegation.Restriction
import com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping
import com.digitalasset.canton.protocol.{
  DynamicSequencingParameters,
  DynamicSynchronizerParameters,
  v30,
}
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag
import com.digitalasset.canton.topology.transaction.TopologyMapping.RequiredAuth.*
import com.digitalasset.canton.topology.transaction.TopologyMapping.{
  Code,
  MappingHash,
  RequiredAuth,
  newSigningKeys,
}
import com.digitalasset.canton.version.ProtoVersion
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError, SequencerAlias}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens
import slick.jdbc.SetParameter

import scala.annotation.nowarn
import scala.math.Ordering.Implicits.*
import scala.reflect.ClassTag

sealed trait TopologyMapping extends Product with Serializable with PrettyPrinting { self =>

  require(maybeUid.forall(_.namespace == namespace), "namespace is inconsistent")

  override protected def pretty: Pretty[this.type] = adHocPrettyInstance

  def companion: TopologyMappingCompanion

  /** Returns the code used to store & index this mapping */
  final def code: Code = companion.code

  /** The "primary" namespace authorizing the topology mapping. Used for filtering query results.
    */
  def namespace: Namespace

  /** The "primary" identity authorizing the topology mapping, optional as some mappings (namespace
    * delegations and decentralized namespace definitions) only have a namespace Used for filtering
    * query results.
    */
  def maybeUid: Option[UniqueIdentifier]

  /** Returns authorization information
    *
    * Each topology transaction must be authorized directly or indirectly by all necessary
    * controllers of the given namespace.
    *
    * @param previous
    *   the previously validly authorized state (some state changes only need subsets of the
    *   authorizers)
    */
  def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth

  def restrictedToSynchronizer: Option[SynchronizerId]

  def toProtoV30: v30.TopologyMapping

  def uniqueKey: MappingHash

  final def select[TargetMapping <: TopologyMapping](implicit
      M: ClassTag[TargetMapping]
  ): Option[TargetMapping] = M.unapply(this)

}

object TopologyMapping {
  def newSigningKeys[T <: KeyMapping](
      current: T,
      previous: Option[TopologyTransaction[TopologyChangeOp.Replace, T]],
  ): Set[SigningPublicKey] = {
    val previouslyRegisteredKeys = previous.toList
      .flatMap(transaction => transaction.mapping.mappedSigningKeys)
      .toSet

    current.mappedSigningKeys -- previouslyRegisteredKeys
  }

  def participantIdFromProtoPrimitive(
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

  sealed abstract class Code private (
      val code: String,
      val toProtoV30: v30.Enums.TopologyMappingCode,
  ) extends Product
      with Serializable {
    @inline final def dbInt: Int = toProtoV30.value
  }

  object Code {
    import v30.Enums.TopologyMappingCode as v30Code
    case object NamespaceDelegation
        extends Code("nsd", v30Code.TOPOLOGY_MAPPING_CODE_NAMESPACE_DELEGATION)
    case object DecentralizedNamespaceDefinition
        extends Code("dnd", v30Code.TOPOLOGY_MAPPING_CODE_DECENTRALIZED_NAMESPACE_DEFINITION)

    case object OwnerToKeyMapping
        extends Code("otk", v30Code.TOPOLOGY_MAPPING_CODE_OWNER_TO_KEY_MAPPING)

    case object SynchronizerTrustCertificate
        extends Code("dtc", v30Code.TOPOLOGY_MAPPING_CODE_SYNCHRONIZER_TRUST_CERTIFICATE)
    case object ParticipantSynchronizerPermission
        extends Code("pdp", v30Code.TOPOLOGY_MAPPING_CODE_PARTICIPANT_PERMISSION)
    case object PartyHostingLimits
        extends Code("phl", v30Code.TOPOLOGY_MAPPING_CODE_PARTY_HOSTING_LIMITS)
    case object VettedPackages extends Code("vtp", v30Code.TOPOLOGY_MAPPING_CODE_VETTED_PACKAGES)

    case object PartyToParticipant
        extends Code("ptp", v30Code.TOPOLOGY_MAPPING_CODE_PARTY_TO_PARTICIPANT)

    case object SynchronizerParametersState
        extends Code("dop", v30Code.TOPOLOGY_MAPPING_CODE_SYNCHRONIZER_PARAMETERS_STATE)
    case object MediatorSynchronizerState
        extends Code("mds", v30Code.TOPOLOGY_MAPPING_CODE_MEDIATOR_SYNCHRONIZER_STATE)
    case object SequencerSynchronizerState
        extends Code("sds", v30Code.TOPOLOGY_MAPPING_CODE_SEQUENCER_SYNCHRONIZER_STATE)

    case object SequencingDynamicParametersState
        extends Code("sep", v30Code.TOPOLOGY_MAPPING_CODE_SEQUENCING_DYNAMIC_PARAMETERS_STATE)
    case object PartyToKeyMapping
        extends Code("ptk", v30Code.TOPOLOGY_MAPPING_CODE_PARTY_TO_KEY_MAPPING)

    case object SynchronizerUpgradeAnnouncement
        extends Code("sua", v30Code.TOPOLOGY_MAPPING_CODE_SYNCHRONIZER_MIGRATION_ANNOUNCEMENT)
    case object SequencerConnectionSuccessor
        extends Code("scs", v30Code.TOPOLOGY_MAPPING_CODE_SEQUENCER_CONNECTION_SUCCESSOR)

    val all: Seq[Code] = Seq(
      NamespaceDelegation,
      DecentralizedNamespaceDefinition,
      OwnerToKeyMapping,
      SynchronizerTrustCertificate,
      ParticipantSynchronizerPermission,
      PartyHostingLimits,
      VettedPackages,
      PartyToParticipant,
      SynchronizerParametersState,
      MediatorSynchronizerState,
      SequencerSynchronizerState,
      SequencingDynamicParametersState,
      PartyToKeyMapping,
      SynchronizerUpgradeAnnouncement,
      SequencerConnectionSuccessor,
    )

    val logicalSynchronizerUpgradeMappings: Set[Code] =
      Set[Code](Code.SynchronizerUpgradeAnnouncement, Code.SequencerConnectionSuccessor)

    def fromString(code: String): ParsingResult[Code] =
      all
        .find(_.code == code)
        .toRight(UnrecognizedEnum("TopologyMapping.Code", code, all.map(_.code)))

    def fromProtoV30(code: v30.Enums.TopologyMappingCode): ParsingResult[Code] =
      all
        .find(_.toProtoV30 == code)
        .toRight(UnrecognizedEnum("Enumis.TopologyMappingCode", code.value))

    implicit val setParameterTopologyMappingCode: SetParameter[Code] =
      (v, pp) => pp.setInt(v.dbInt)

  }

  // Small wrapper to not have to work with a tuple3 (Set[Namespace], Set[Uid], Set[Fingerprint])
  final case class ReferencedAuthorizations(
      namespaces: Set[Namespace] = Set.empty,
      extraKeys: Set[Fingerprint] = Set.empty,
  ) extends PrettyPrinting {
    def isEmpty: Boolean =
      namespaces.isEmpty && extraKeys.isEmpty

    override protected def pretty: Pretty[ReferencedAuthorizations.this.type] = prettyOfClass(
      paramIfNonEmpty("namespaces", _.namespaces),
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
            namespaces = x.namespaces ++ y.namespaces,
            extraKeys = x.extraKeys ++ y.extraKeys,
          )
      }
  }

  sealed trait RequiredAuth extends PrettyPrinting {
    def satisfiedByActualAuthorizers(
        provided: ReferencedAuthorizations
    ): Either[ReferencedAuthorizations, Unit]

    final def or(next: RequiredAuth): RequiredAuth =
      RequiredAuth.Or(this, next)

    final def and(next: RequiredAuth): RequiredAuth = (this, next) match {
      // simple optimization that squashes And(RequiredNamespaces, RequiredNamespaces) trivially into just RequiredNamespaces
      case (a: RequiredNamespaces, b: RequiredNamespaces) =>
        RequiredNamespaces(
          namespaces = a.namespaces ++ b.namespaces,
          extraKeys = a.extraKeys ++ b.extraKeys,
        )
      case _ =>
        RequiredAuth.And(this, next)
    }

    /** Authorizations referenced by this instance. Note that the result is not equivalent to this
      * instance, as an "or" gets translated to an "and". Instead, the result indicates which
      * authorization keys need to be evaluated in order to check if this RequiredAuth is met.
      */
    def referenced: ReferencedAuthorizations

  }

  object RequiredAuth {

    final case class RequiredNamespaces(
        namespaces: Set[Namespace],
        extraKeys: Set[Fingerprint] = Set.empty,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] = {
        val missingNamespaces = namespaces.filter(ns => !provided.namespaces(ns))
        val missingKeys = extraKeys.filter(key => !provided.extraKeys(key))
        Either.cond(
          missingNamespaces.isEmpty && missingKeys.isEmpty,
          (),
          ReferencedAuthorizations(namespaces = missingNamespaces, extraKeys = missingKeys),
        )
      }

      override lazy val referenced: ReferencedAuthorizations =
        ReferencedAuthorizations(namespaces = namespaces, extraKeys = extraKeys)

      override protected def pretty: Pretty[RequiredNamespaces.this.type] = prettyOfClass(
        unnamedParam(_.namespaces.toSeq.sortBy(_.toProtoPrimitive)),
        paramIfNonEmpty("extra keys", _.extraKeys.toSeq.sortBy(_.toProtoPrimitive)),
      )
    }

    object RequiredNamespaces {
      def apply(hasNamespace: HasNamespace*): RequiredNamespaces = RequiredNamespaces(
        hasNamespace.map(_.namespace).toSet
      )
    }

    private[topology] final case class Or(
        first: RequiredAuth,
        second: RequiredAuth,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] =
        first
          .satisfiedByActualAuthorizers(provided)
          .orElse(second.satisfiedByActualAuthorizers(provided))

      override lazy val referenced: ReferencedAuthorizations =
        ReferencedAuthorizations.monoid.combine(first.referenced, second.referenced)

      override protected def pretty: Pretty[Or.this.type] =
        prettyOfString(_ => show"($first || $second)")
    }

    private[topology] final case class And(
        first: RequiredAuth,
        second: RequiredAuth,
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] =
        first
          .satisfiedByActualAuthorizers(provided)
          .toEitherNel
          .combine(second.satisfiedByActualAuthorizers(provided).toEitherNel)
          .leftMap(_.reduce)

      override def referenced: ReferencedAuthorizations =
        ReferencedAuthorizations.monoid.combine(first.referenced, second.referenced)

      override protected def pretty: Pretty[And.this.type] =
        prettyOfString(_ => show"($first && $second)")
    }

  }

  def fromProtoV30(proto: v30.TopologyMapping): ParsingResult[TopologyMapping] =
    proto.mapping match {
      case Mapping.Empty =>
        FieldNotSet("mapping").asLeft
      case Mapping.NamespaceDelegation(value) => NamespaceDelegation.fromProtoV30(value)
      case Mapping.DecentralizedNamespaceDefinition(value) =>
        DecentralizedNamespaceDefinition.fromProtoV30(value)
      case Mapping.OwnerToKeyMapping(value) => OwnerToKeyMapping.fromProtoV30(value)
      case Mapping.PartyToKeyMapping(value) => PartyToKeyMapping.fromProtoV30(value)
      case Mapping.SynchronizerTrustCertificate(value) =>
        SynchronizerTrustCertificate.fromProtoV30(value)
      case Mapping.PartyHostingLimits(value) => PartyHostingLimits.fromProtoV30(value)
      case Mapping.ParticipantPermission(value) =>
        ParticipantSynchronizerPermission.fromProtoV30(value)
      case Mapping.VettedPackages(value) => VettedPackages.fromProtoV30(value)
      case Mapping.PartyToParticipant(value) => PartyToParticipant.fromProtoV30(value)
      case Mapping.SynchronizerParametersState(value) =>
        SynchronizerParametersState.fromProtoV30(value)
      case Mapping.SequencingDynamicParametersState(value) =>
        DynamicSequencingParametersState.fromProtoV30(value)
      case Mapping.MediatorSynchronizerState(value) => MediatorSynchronizerState.fromProtoV30(value)
      case Mapping.SequencerSynchronizerState(value) =>
        SequencerSynchronizerState.fromProtoV30(value)
      case Mapping.SynchronizerUpgradeAnnouncement(value) =>
        SynchronizerUpgradeAnnouncement.fromProtoV30(value)
      case Mapping.SequencerConnectionSuccessor(value) =>
        SequencerConnectionSuccessor.fromProtoV30(value)
    }
}

/** Trait for all companion objects of topology mappings. This allows for a nicer console UX,
  * because users can just refer to, for example, OwnerToKeyMapping instead of
  * TopologyMapping.Code.OwnerToKeyMapping or OwnerToKeyMapping.code.
  */
sealed trait TopologyMappingCompanion extends Serializable {
  def code: Code

  require(Code.all.contains(code), s"The code for $this is not listed in TopologyMapping.Code.all")
}

/** Represents a restriction of namespace to specific mapping types.
  */
sealed trait DelegationRestriction extends Product with Serializable {
  def canSign(mappingToSign: Code): Boolean
  def toProtoV30: v30.NamespaceDelegation.Restriction
}
object DelegationRestriction {

  /** If no mapping restrictions are specified, returns CanSignAllMappings.
    */
  def fromProtoV30(
      restriction: v30.NamespaceDelegation.Restriction
  ): ParsingResult[Option[DelegationRestriction]] =
    restriction match {
      case Restriction.Empty => ParsingResult.pure(None)
      case Restriction.CanSignAllMappings(v30.NamespaceDelegation.CanSignAllMappings()) =>
        ParsingResult.pure(Some(CanSignAllMappings))
      case Restriction.CanSignAllButNamespaceDelegations(
            v30.NamespaceDelegation.CanSignAllButNamespaceDelegations()
          ) =>
        ParsingResult.pure(Some(CanSignAllButNamespaceDelegations))
      case Restriction.CanSignSpecificMapings(
            v30.NamespaceDelegation.CanSignSpecificMappings(mappings)
          ) =>
        ProtoConverter
          .parseRequiredNonEmpty(Code.fromProtoV30, "mappings", mappings)
          .map(restrictions => Some(CanSignSpecificMappings(restrictions.toSet)))
    }

  /** Indicates that there are no mapping restrictions and is represented by no restrictions being
    * specified in the proto message. The target key of the delegation will also be permitted to
    * sign topology mappings that are added in future releases.
    */
  case object CanSignAllMappings extends DelegationRestriction {
    override def canSign(mappingToSign: Code) = true

    override def toProtoV30: v30.NamespaceDelegation.Restriction =
      v30.NamespaceDelegation.Restriction.CanSignAllMappings(
        v30.NamespaceDelegation.CanSignAllMappings()
      )
  }

  /** Indicates that the key can be used to sign all mappings except for namespace delegations. The
    * target key of the delegation will also be permitted to sign topology mappings that are added
    * in future releases.
    */
  case object CanSignAllButNamespaceDelegations extends DelegationRestriction {
    override def canSign(mappingToSign: Code): Boolean = mappingToSign != Code.NamespaceDelegation

    override def toProtoV30: v30.NamespaceDelegation.Restriction =
      v30.NamespaceDelegation.Restriction.CanSignAllButNamespaceDelegations(
        v30.NamespaceDelegation.CanSignAllButNamespaceDelegations()
      )
  }

  /** Indicates that the target key of the delegation can only be used to sign specific mappings.
    * The delegation will have to be updated in case the target key of the delegation should be
    * allowed to sign topology mappings that are added in future releases.
    * @param mappings
    *   the mappings the delegation is restricted to.
    */
  final case class CanSignSpecificMappings(mappings: NonEmpty[Set[Code]])
      extends DelegationRestriction {
    override def canSign(mappingToSign: Code): Boolean = mappings.contains(mappingToSign)

    override def toProtoV30: v30.NamespaceDelegation.Restriction =
      v30.NamespaceDelegation.Restriction.CanSignSpecificMapings(
        v30.NamespaceDelegation.CanSignSpecificMappings(
          mappings.map(_.toProtoV30).toSeq.sortBy(_.value)
        )
      )
  }

  object CanSignSpecificMappings {
    def apply(code: Code, codes: Code*): CanSignSpecificMappings = CanSignSpecificMappings(
      NonEmpty(Set, code, codes*)
    )

    def apply(
        code: TopologyMappingCompanion,
        codes: TopologyMappingCompanion*
    ): CanSignSpecificMappings = CanSignSpecificMappings(
      NonEmpty(Set, code, codes*).map(_.code)
    )
  }
}

/** A namespace delegation transaction (intermediate CA)
  *
  * Entrusts a public-key to perform changes on the namespace {(*,I) => p_k}
  *
  * The key can be restricted to only sign specific mapping types.
  */
final case class NamespaceDelegation private (
    namespace: Namespace,
    target: SigningPublicKey,
    restriction: DelegationRestriction,
) extends TopologyMapping {

  override def companion: NamespaceDelegation.type = NamespaceDelegation

  def toProto: v30.NamespaceDelegation =
    v30.NamespaceDelegation(
      namespace = namespace.fingerprint.unwrap,
      targetKey = Some(target.toProtoV30),
      // never set the isRootDelegation flag to true
      isRootDelegation = false,
      restriction = restriction.toProtoV30,
    )

  def canSign(mappingsToSign: Code): Boolean =
    restriction.canSign(mappingsToSign)

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.NamespaceDelegation(
        toProto
      )
    )

  override def maybeUid: Option[UniqueIdentifier] = None

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(Set(namespace))

  override lazy val uniqueKey: MappingHash =
    NamespaceDelegation.uniqueKey(namespace, target.fingerprint)

  @VisibleForTesting
  private[transaction] def copy(
      namespace: Namespace = namespace,
      target: SigningPublicKey = target,
      restriction: DelegationRestriction = restriction,
  ) = new NamespaceDelegation(namespace, target, restriction)
}

object NamespaceDelegation extends TopologyMappingCompanion {

  def uniqueKey(namespace: Namespace, target: Fingerprint): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap).add(target.unwrap))

  /** Creates a namespace delegation for the given namespace to the given target key with possible
    * restrictions on the topology mappings the target key can authorize.
    *
    * @param namespace
    *   the namespace for which the target key may sign topology transaction
    * @param target
    *   the key must have `Namespace` listed as a usage to be eligible as the target of a namespace
    *   delegation.
    * @param restrictedToMappings
    *   mappings that the target key of this delegation is allowed to sign
    */
  def create(
      namespace: Namespace,
      target: SigningPublicKey,
      restriction: DelegationRestriction,
  ): Either[String, NamespaceDelegation] =
    for {
      _ <- Either.cond(
        SigningKeyUsage.matchesRelevantUsages(target.usage, SigningKeyUsage.NamespaceOnly),
        (),
        s"The key ${target.id} must include a ${SigningKeyUsage.Namespace} usage.",
      )
      namespaceDelegation <- Either.cond(
        restriction.canSign(Code.NamespaceDelegation) ||
          namespace.fingerprint != target.fingerprint,
        NamespaceDelegation(namespace, target, restriction),
        s"Root certificate for $namespace needs to be be able to sign other NamespaceDelegations",
      )

    } yield namespaceDelegation

  @VisibleForTesting
  private[canton] def tryCreate(
      namespace: Namespace,
      target: SigningPublicKey,
      restriction: DelegationRestriction,
  ): NamespaceDelegation =
    create(namespace, target, restriction).valueOr(err => throw new IllegalArgumentException((err)))

  override def code: TopologyMapping.Code = Code.NamespaceDelegation

  /** Returns true if the given transaction is a self-signed root certificate */
  def isRootCertificate(sit: GenericSignedTopologyTransaction): Boolean =
    sit.mapping
      .select[transaction.NamespaceDelegation]
      .exists(ns =>
        // a root certificate must only be signed by the namespace key, but we accept multiple signatures from that key
        sit.signatures.forall(_.authorizingLongTermKey == ns.namespace.fingerprint) &&
          // explicitly checking for nonEmpty to guard against refactorings away from NonEmpty[Set[...]].
          sit.signatures.nonEmpty &&
          ns.canSign(Code.NamespaceDelegation) &&
          ns.target.fingerprint == ns.namespace.fingerprint
      )

  @nowarn("cat=deprecation")
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
      explicitRestriction <- DelegationRestriction.fromProtoV30(value.restriction)
      finalRestriction <- explicitRestriction match {
        case None =>
          // this branch is for maintaining backwards compatibility
          ParsingResult.pure[DelegationRestriction](
            if (value.isRootDelegation) CanSignAllMappings
            else CanSignAllButNamespaceDelegations
          )
        case Some(restriction) =>
          Either.cond(
            !value.isRootDelegation,
            restriction,
            // if a restriction was set, then is_root_delegation must not be set to true
            ProtoDeserializationError.InvariantViolation(
              "is_root_delegation",
              "is_root_delegation was set to true, but the list of mapping restrictions did not contain NamespaceDelegation",
            ),
          )
      }
      namespaceDelegation <- NamespaceDelegation
        .create(namespace, target, finalRestriction)
        .leftMap(err => ProtoDeserializationError.InvariantViolation(None, err))

    } yield namespaceDelegation

  @VisibleForTesting
  val restrictionUnsafe: Lens[NamespaceDelegation, DelegationRestriction] =
    GenLens[NamespaceDelegation](_.restriction)

}

/** Defines a decentralized namespace
  *
  * authorization: whoever controls the synchronizer and all the owners of the active or observing
  * sequencers that were not already present in the tx with serial = n - 1 exception: a sequencer
  * can leave the consortium unilaterally as long as there are enough members to reach the threshold
  */
final case class DecentralizedNamespaceDefinition private (
    override val namespace: Namespace,
    threshold: PositiveInt,
    owners: NonEmpty[Set[Namespace]],
) extends TopologyMapping {

  override def companion: DecentralizedNamespaceDefinition.type = DecentralizedNamespaceDefinition

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

  override def maybeUid: Option[UniqueIdentifier] = None

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

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

object DecentralizedNamespaceDefinition extends TopologyMappingCompanion {

  def uniqueKey(namespace: Namespace): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap))

  override def code: TopologyMapping.Code = Code.DecentralizedNamespaceDefinition

  def tryCreate(
      decentralizedNamespace: Namespace,
      threshold: PositiveInt,
      owners: NonEmpty[Set[Namespace]],
  ): DecentralizedNamespaceDefinition =
    create(decentralizedNamespace, threshold, owners).valueOr(err =>
      throw new IllegalArgumentException((err))
    )

  def create(
      decentralizedNamespace: Namespace,
      threshold: PositiveInt,
      owners: NonEmpty[Set[Namespace]],
  ): Either[String, DecentralizedNamespaceDefinition] =
    for {
      _ <- Either.cond(
        owners.sizeIs >= threshold.value,
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
    Namespace(Fingerprint.tryFromString(builder.finish().toLengthLimitedHexString))
  }
}

/** A topology mapping that maps to a set of public keys for which ownership has to be proven. A
  * KeyMapping can also specify a SigningPublicKey that can be used for authorizing the mappings
  * namespace.
  */
sealed trait KeyMapping extends TopologyMapping with Product with Serializable {
  def mappedKeys: Set[PublicKey]
  final def mappedSigningKeys: Set[SigningPublicKey] = mappedKeys.collect {
    case k: SigningPublicKey => k
  }

  def namespaceKeyForSelfAuthorization: Option[SigningPublicKey] = None

  def isSelfSigned = namespaceKeyForSelfAuthorization.nonEmpty

  require(namespaceKeyForSelfAuthorization.forall(_.fingerprint == namespace.fingerprint))
}

object KeyMapping {
  val MaxKeys: Int = 20

  def validateKeysSizeSet(
      keys: NonEmpty[Set[SigningPublicKey]],
      maxKeys: Int,
  ): Either[String, Unit] =
    Either.cond(keys.sizeIs <= maxKeys, (), s"At most $maxKeys can be specified.")

  def validateKeysSize(
      keys: NonEmpty[Seq[PublicKey]],
      maxKeys: Int,
  ): Either[String, Unit] =
    for {
      _ <- Either.cond(keys.sizeIs <= maxKeys, (), s"At most $maxKeys can be specified.")
    } yield ()
}

/** A key owner (participant, mediator, sequencer) to key mapping
  *
  * In Canton, we need to know keys for all participating entities. The entities are all the
  * protocol members (participant, mediator) plus the sequencer (which provides the communication
  * infrastructure for the protocol members).
  */
final case class OwnerToKeyMapping private (
    member: Member,
    keys: NonEmpty[Seq[PublicKey]],
) extends TopologyMapping
    with KeyMapping {

  override def companion: OwnerToKeyMapping.type = OwnerToKeyMapping

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

  override def namespace: Namespace = member.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(member.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = {
    val newKeys =
      newSigningKeys(this, previous.flatMap(_.select[TopologyChangeOp.Replace, OwnerToKeyMapping]))
        .map(_.fingerprint)
    RequiredNamespaces(Set(member.namespace), extraKeys = newKeys)
  }

  override def uniqueKey: MappingHash = OwnerToKeyMapping.uniqueKey(member)

  override def mappedKeys: Set[PublicKey] = keys.forgetNE.toSet[PublicKey]

  @VisibleForTesting
  private[transaction] def copy(
      member: Member = member,
      keys: NonEmpty[Seq[PublicKey]] = keys,
  ) =
    new OwnerToKeyMapping(member, keys)
}

object OwnerToKeyMapping extends TopologyMappingCompanion {

  val MaxKeys: Int = KeyMapping.MaxKeys

  @VisibleForTesting
  val keysUnsafe: Lens[OwnerToKeyMapping, NonEmpty[Seq[PublicKey]]] =
    GenLens[OwnerToKeyMapping](_.keys)

  def uniqueKey(member: Member): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(member.uid.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.OwnerToKeyMapping

  def create(member: Member, keys: NonEmpty[Seq[PublicKey]]): Either[String, OwnerToKeyMapping] = {
    val duplicateKeys = keys.groupBy(_.fingerprint).values.filter(_.sizeIs > 1).toList
    for {
      _ <- Either.cond(
        duplicateKeys.isEmpty,
        (),
        s"All keys must be unique. Duplicate keys: $duplicateKeys",
      )
      _ <- KeyMapping.validateKeysSize(keys, MaxKeys)
    } yield OwnerToKeyMapping(member, keys)
  }

  @VisibleForTesting
  def tryCreate(member: Member, keys: NonEmpty[Seq[PublicKey]]): OwnerToKeyMapping =
    create(member, keys).valueOr(err => throw new IllegalArgumentException(err))

  def fromProtoV30(
      value: v30.OwnerToKeyMapping
  ): ParsingResult[OwnerToKeyMapping] = {
    val v30.OwnerToKeyMapping(memberP, keysP) = value
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
      keys <- ProtoConverter
        .parseRequiredNonEmpty(PublicKey.fromProtoPublicKeyV30, "public_keys", keysP)
      otk <- create(member, keys).leftMap(ProtoDeserializationError.InvariantViolation(None, _))
    } yield otk
  }
}

/** A party to key mapping
  *
  * In Canton, we can delegate the submission authorisation to a participant node, or we can sign
  * the transaction outside of the node with a party key. This mapping is used to map the party to a
  * set of public keys authorized to sign submissions.
  */
final case class PartyToKeyMapping private (
    party: PartyId,
    signingKeysWithThreshold: SigningKeysWithThreshold,
) extends TopologyMapping
    with KeyMapping {

  override def companion: PartyToKeyMapping.type = PartyToKeyMapping

  def toProto: v30.PartyToKeyMapping = v30.PartyToKeyMapping(
    party = party.toProtoPrimitive,
    threshold = signingKeysWithThreshold.threshold.unwrap,
    signingKeys = signingKeysWithThreshold.keys.toSeq.sortBy(_.fingerprint).map(_.toProtoV30),
  )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyToKeyMapping(
        toProto
      )
    )

  @VisibleForTesting
  def tryCopy(
      party: PartyId = party,
      threshold: PositiveInt = signingKeysWithThreshold.threshold,
      signingKeys: NonEmpty[Set[SigningPublicKey]] = signingKeysWithThreshold.keys,
  ): PartyToKeyMapping =
    PartyToKeyMapping.tryCreate(
      party,
      threshold,
      signingKeys.toSeq,
    )

  def signingKeys: NonEmpty[Set[SigningPublicKey]] = signingKeysWithThreshold.keys

  def threshold: PositiveInt = signingKeysWithThreshold.threshold

  override def namespace: Namespace = party.namespace

  override def maybeUid: Option[UniqueIdentifier] = Some(party.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = {
    val newKeys = TopologyMapping.newSigningKeys(
      this,
      previous.flatMap(_.select[TopologyChangeOp.Replace, PartyToKeyMapping]),
    )

    RequiredNamespaces(Set(party.namespace), newKeys.map(_.fingerprint))
  }

  override def uniqueKey: MappingHash = PartyToKeyMapping.uniqueKey(party)

  override def mappedKeys: Set[PublicKey] = signingKeysWithThreshold.keys.forgetNE.toSet[PublicKey]

  @VisibleForTesting
  private[transaction] def copy(
      party: PartyId = party,
      signingKeysWithThreshold: SigningKeysWithThreshold = signingKeysWithThreshold,
  ) =
    new PartyToKeyMapping(party, signingKeysWithThreshold)

  @VisibleForTesting
  def copySigningKeysUnsafe(signingKeysWithThreshold: SigningKeysWithThreshold): PartyToKeyMapping =
    PartyToKeyMapping(
      party = party,
      signingKeysWithThreshold = signingKeysWithThreshold,
    )
}

object PartyToKeyMapping extends TopologyMappingCompanion {

  val MaxKeys: Int = KeyMapping.MaxKeys

  def create(
      partyId: PartyId,
      threshold: PositiveInt,
      signingKeys: NonEmpty[Seq[SigningPublicKey]],
  ): Either[String, PartyToKeyMapping] = {
    val signingKeysWithThreshold = SigningKeysWithThreshold(signingKeys.toSet, threshold)
    for {
      // The toSet removes duplicate signing keys before creating the SigningKeysWithThreshold
      // This is on purpose here because legacy existing P2Ks may have duplicate keys and we must be able to deserialize them
      _ <- KeyMapping.validateKeysSizeSet(signingKeysWithThreshold.keys, MaxKeys)
    } yield PartyToKeyMapping(partyId, signingKeysWithThreshold)
  }

  def tryCreate(
      partyId: PartyId,
      threshold: PositiveInt,
      signingKeys: NonEmpty[Seq[SigningPublicKey]],
  ): PartyToKeyMapping =
    create(partyId, threshold, signingKeys).valueOr(err => throw new IllegalArgumentException(err))

  def uniqueKey(party: PartyId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(b => b.add(party.uid.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.PartyToKeyMapping

  def fromProtoV30(
      value: v30.PartyToKeyMapping
  ): ParsingResult[PartyToKeyMapping] = {
    val v30.PartyToKeyMapping(partyP, thresholdP, signingKeysP) = value
    for {
      party <- PartyId.fromProtoPrimitive(partyP, "party")
      signingKeysNE <-
        ProtoConverter.parseRequiredNonEmpty(
          SigningPublicKey.fromProtoV30,
          "signing_keys",
          signingKeysP,
        )
      threshold <- PositiveInt
        .create(thresholdP)
        .leftMap(InvariantViolation.toProtoDeserializationError("threshold", _))
      ptk <- PartyToKeyMapping
        .create(party, threshold, signingKeysNE)
        .leftMap(ProtoDeserializationError.InvariantViolation(None, _))
    } yield ptk
  }

}

/** Participant synchronizer trust certificate
  * @param featureFlags
  *   Protocol features supported by [[participantId]] on [[synchronizerId]]. Feature flags are used
  *   to add targeted support for a protocol feature or bugfix without requiring a new protocol
  *   version. Care must be taken to not create ledger forks when using such flags.
  */
final case class SynchronizerTrustCertificate(
    participantId: ParticipantId,
    synchronizerId: SynchronizerId,
    featureFlags: Seq[ParticipantTopologyFeatureFlag] = Seq.empty,
) extends TopologyMapping {

  override def companion: SynchronizerTrustCertificate.type = SynchronizerTrustCertificate

  def toProto: v30.SynchronizerTrustCertificate =
    v30.SynchronizerTrustCertificate(
      participantUid = participantId.uid.toProtoPrimitive,
      synchronizerId = synchronizerId.toProtoPrimitive,
      featureFlags = featureFlags.map(_.toProtoV30),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SynchronizerTrustCertificate(
        toProto
      )
    )

  override def namespace: Namespace = participantId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredNamespaces(participantId)

  override def uniqueKey: MappingHash =
    SynchronizerTrustCertificate.uniqueKey(participantId, synchronizerId)
}

object SynchronizerTrustCertificate extends TopologyMappingCompanion {
  final case class ParticipantTopologyFeatureFlag private (value: Int)(
      name: Option[String] = None
  ) {
    def toProtoV30: v30.Enums.ParticipantFeatureFlag =
      v30.Enums.ParticipantFeatureFlag.fromValue(value)
    override def toString: String = name.getOrElse(s"UnrecognizedFeatureFlag($value)")
  }

  object ParticipantTopologyFeatureFlag {

    /** Feature flag enabled when the participant supports the fix for a bug that incorrectly
      * rejects externally signed transactions with a locally created contract used in a subview.
      * See https://github.com/DACH-NY/canton/issues/27883 Used only in PV33.
      */
    val ExternalSigningLocalContractsInSubview: ParticipantTopologyFeatureFlag =
      ParticipantTopologyFeatureFlag(
        v30.Enums.ParticipantFeatureFlag.PARTICIPANT_FEATURE_FLAG_PV33_EXTERNAL_SIGNING_LOCAL_CONTRACT_IN_SUBVIEW.value
      )(Some("ExternalSigningLocalContractsInSubview"))

    val knownTopologyFeatureFlags: Seq[ParticipantTopologyFeatureFlag] = Seq(
      ExternalSigningLocalContractsInSubview
    )

    def fromProtoV30(
        valueP: v30.Enums.ParticipantFeatureFlag
    ): Option[ParticipantTopologyFeatureFlag] =
      knownTopologyFeatureFlags
        .find(_.value == valueP.value)
        .orElse(
          Option.when(valueP != ParticipantFeatureFlag.PARTICIPANT_FEATURE_FLAG_UNSPECIFIED)(
            ParticipantTopologyFeatureFlag(valueP.value)()
          )
        )
  }

  def uniqueKey(participantId: ParticipantId, synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(participantId.toProtoPrimitive).add(synchronizerId.toProtoPrimitive)
    )

  override def code: Code = Code.SynchronizerTrustCertificate

  def fromProtoV30(
      valueP: v30.SynchronizerTrustCertificate
  ): ParsingResult[SynchronizerTrustCertificate] =
    for {
      participantId <- TopologyMapping.participantIdFromProtoPrimitive(
        valueP.participantUid,
        "participant_uid",
      )
      synchronizerId <- SynchronizerId.fromProtoPrimitive(valueP.synchronizerId, "synchronizer_id")
      featureFlags = valueP.featureFlags.flatMap(ParticipantTopologyFeatureFlag.fromProtoV30)
    } yield SynchronizerTrustCertificate(
      participantId,
      synchronizerId,
      featureFlags,
    )
}

/** Permissions of a participant, i.e., things a participant can do on behalf of a party
  *
  * Permissions are hierarchical. A participant who can submit can confirm. A participant who can
  * confirm can observe.
  */
sealed trait ParticipantPermission extends Product with Serializable {
  def toProtoV30: v30.Enums.ParticipantPermission

  // For a participant to be able to confirm, having Submission or Confirmation permission
  // is a necessary, but not sufficient condition (e.g. HostingParticipant.onboarding must
  // be false). To prevent accidental use of this method, make canConfirm package-private.
  private[transaction] def canConfirm: Boolean
}
object ParticipantPermission {
  case object Submission extends ParticipantPermission {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
    private[transaction] def canConfirm: Boolean = true
  }
  case object Confirmation extends ParticipantPermission {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
    private[transaction] def canConfirm: Boolean = true
  }
  case object Observation extends ParticipantPermission {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
    private[transaction] def canConfirm: Boolean = false
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

/** @param confirmationRequestsMaxRate
  *   maximum number of mediator confirmation requests sent per participant per second
  */
final case class ParticipantSynchronizerLimits(
    confirmationRequestsMaxRate: NonNegativeInt
) extends PrettyPrinting {

  override protected def pretty: Pretty[ParticipantSynchronizerLimits] =
    prettyOfClass(
      param("confirmation requests max rate", _.confirmationRequestsMaxRate)
    )

  def toProto: v30.ParticipantSynchronizerLimits =
    v30.ParticipantSynchronizerLimits(confirmationRequestsMaxRate.unwrap)
}
object ParticipantSynchronizerLimits {
  def fromProtoV30(
      value: v30.ParticipantSynchronizerLimits
  ): ParsingResult[ParticipantSynchronizerLimits] =
    for {
      confirmationRequestsMaxRate <- NonNegativeInt
        .create(value.confirmationRequestsMaxRate)
        .leftMap(ProtoDeserializationError.InvariantViolation("confirmation_requests_max_rate", _))
    } yield ParticipantSynchronizerLimits(confirmationRequestsMaxRate)
}
final case class ParticipantSynchronizerPermission(
    synchronizerId: SynchronizerId,
    participantId: ParticipantId,
    permission: ParticipantPermission,
    limits: Option[ParticipantSynchronizerLimits],
    loginAfter: Option[CantonTimestamp],
) extends TopologyMapping {

  override def companion: ParticipantSynchronizerPermission.type = ParticipantSynchronizerPermission

  def toProto: v30.ParticipantSynchronizerPermission =
    v30.ParticipantSynchronizerPermission(
      synchronizerId = synchronizerId.toProtoPrimitive,
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

  override def namespace: Namespace = participantId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredNamespaces(synchronizerId)

  override def uniqueKey: MappingHash =
    ParticipantSynchronizerPermission.uniqueKey(synchronizerId, participantId)

  def setDefaultLimitIfNotSet(
      defaultLimits: ParticipantSynchronizerLimits
  ): ParticipantSynchronizerPermission =
    if (limits.nonEmpty)
      this
    else
      ParticipantSynchronizerPermission(
        synchronizerId,
        participantId,
        permission,
        Some(defaultLimits),
        loginAfter,
      )
}

object ParticipantSynchronizerPermission extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId, participantId: ParticipantId): MappingHash =
    TopologyMapping.buildUniqueKey(
      code
    )(_.add(synchronizerId.toProtoPrimitive).add(participantId.toProtoPrimitive))

  override def code: Code = Code.ParticipantSynchronizerPermission

  def default(
      synchronizerId: SynchronizerId,
      participantId: ParticipantId,
  ): ParticipantSynchronizerPermission =
    ParticipantSynchronizerPermission(
      synchronizerId,
      participantId,
      ParticipantPermission.Submission,
      None,
      None,
    )

  def fromProtoV30(
      valueP: v30.ParticipantSynchronizerPermission
  ): ParsingResult[ParticipantSynchronizerPermission] =
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(valueP.synchronizerId, "synchronizer_id")
      participantId <- TopologyMapping.participantIdFromProtoPrimitive(
        valueP.participantUid,
        "participant_uid",
      )
      permission <- ParticipantPermission.fromProtoV30(valueP.permission)
      limits <- valueP.limits.traverse(ParticipantSynchronizerLimits.fromProtoV30)
      loginAfter <- valueP.loginAfter.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield ParticipantSynchronizerPermission(
      synchronizerId,
      participantId,
      permission,
      limits,
      loginAfter,
    )
}

// Party hosting limits
final case class PartyHostingLimits(
    synchronizerId: SynchronizerId,
    partyId: PartyId,
) extends TopologyMapping {

  override def companion: PartyHostingLimits.type = PartyHostingLimits

  def toProto: v30.PartyHostingLimits =
    v30.PartyHostingLimits(
      synchronizerId = synchronizerId.toProtoPrimitive,
      party = partyId.toProtoPrimitive,
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyHostingLimits(
        toProto
      )
    )

  override def namespace: Namespace = partyId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredNamespaces(synchronizerId)

  override def uniqueKey: MappingHash = PartyHostingLimits.uniqueKey(synchronizerId, partyId)
}

object PartyHostingLimits extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId, partyId: PartyId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(synchronizerId.toProtoPrimitive).add(partyId.toProtoPrimitive)
    )

  override def code: Code = Code.PartyHostingLimits

  def fromProtoV30(
      valueP: v30.PartyHostingLimits
  ): ParsingResult[PartyHostingLimits] =
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(valueP.synchronizerId, "synchronizer_id")
      partyId <- PartyId.fromProtoPrimitive(valueP.party, "party")
    } yield PartyHostingLimits(synchronizerId, partyId)
}

/** Represents a package with an optional validity period. No start or end means that the validity
  * of the package is unbounded. The validity period is expected to be compared to the ledger
  * effective time (LET) of Daml transactions.
  * @param packageId
  *   the hash of the package
  * @param validFromInclusive
  *   optional inclusive start of the validity period in LET
  * @param validUntilExclusive
  *   optional exclusive end of the validity period in LET
  *
  * Note that as validFrom and validUntil are in ledger effective time, the boundaries have
  * different semantics from topology transaction validity boundaries.
  */
final case class VettedPackage(
    packageId: LfPackageId,
    validFromInclusive: Option[CantonTimestamp],
    validUntilExclusive: Option[CantonTimestamp],
) extends PrettyPrinting {

  private def isUnbounded: Boolean = validFromInclusive.isEmpty && validUntilExclusive.isEmpty
  def asUnbounded: VettedPackage = if (isUnbounded) this else VettedPackage(packageId, None, None)

  def validAt(ts: CantonTimestamp): Boolean =
    validFromInclusive.forall(_ <= ts) && validUntilExclusive.forall(_ > ts)

  def toProtoV30: v30.VettedPackages.VettedPackage = v30.VettedPackages.VettedPackage(
    packageId,
    validFromInclusive = validFromInclusive.map(_.toProtoTimestamp),
    validUntilExclusive = validUntilExclusive.map(_.toProtoTimestamp),
  )

  override protected def pretty: Pretty[VettedPackage.this.type] = prettyOfClass(
    param("packageId", _.packageId),
    paramIfDefined("validFromInclusive", _.validFromInclusive),
    paramIfDefined("validUntilExclusive", _.validUntilExclusive),
    paramIfTrue("unbounded", vp => vp.validFromInclusive.isEmpty && vp.validUntilExclusive.isEmpty),
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
    validFromInclusive <- value.validFromInclusive.traverse(CantonTimestamp.fromProtoTimestamp)
    validUntilExclusive <- value.validUntilExclusive.traverse(CantonTimestamp.fromProtoTimestamp)
  } yield VettedPackage(pkgId, validFromInclusive, validUntilExclusive)
}

// Package vetting
final case class VettedPackages private (
    participantId: ParticipantId,
    packages: Seq[VettedPackage],
) extends TopologyMapping {

  override def companion: VettedPackages.type = VettedPackages

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

  override def namespace: Namespace = participantId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(participantId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    RequiredNamespaces(participantId)

  override def uniqueKey: MappingHash = VettedPackages.uniqueKey(participantId)
}

object VettedPackages extends TopologyMappingCompanion {

  def uniqueKey(participantId: ParticipantId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(participantId.toProtoPrimitive))

  override def code: Code = Code.VettedPackages

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
      (vp.validFromInclusive, vp.validUntilExclusive).tupled.exists { case (from, until) =>
        from >= until
      }
    )

    for {
      _ <- Either.cond(
        multipleValidityPeriods.isEmpty,
        (),
        s"a package may only have one validity period: ${multipleValidityPeriods.mkString(", ")}",
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
    onboarding: Boolean,
) {
  def canConfirm: Boolean = permission.canConfirm && !onboarding
  def toProto: v30.PartyToParticipant.HostingParticipant =
    v30.PartyToParticipant.HostingParticipant(
      participantUid = participantId.uid.toProtoPrimitive,
      permission = permission.toProtoV30,
      onboarding = Option.when(onboarding)(v30.PartyToParticipant.HostingParticipant.Onboarding()),
    )
}

object HostingParticipant {
  // Helper for "normal" HostingParticipant construction (as the onboarding flag is used narrowly
  // by party replication)
  def apply(participantId: ParticipantId, permission: ParticipantPermission): HostingParticipant =
    HostingParticipant(participantId, permission, onboarding = false)

  def fromProtoV30(
      value: v30.PartyToParticipant.HostingParticipant
  ): ParsingResult[HostingParticipant] = for {
    participantId <- TopologyMapping.participantIdFromProtoPrimitive(
      value.participantUid,
      "participant_uid",
    )
    permission <- ParticipantPermission.fromProtoV30(value.permission)
  } yield HostingParticipant(participantId, permission, value.onboarding.nonEmpty)
}

/** @param partySigningKeysWithThreshold
  *   Signing keys for the party to authorize transactions. Previously captured via PartyToKey
  *   mappings and party NamespaceDelegation. Added in 3.4, kept as option for compatibility
  */
final case class PartyToParticipant private (
    partyId: PartyId,
    threshold: PositiveInt,
    participants: Seq[HostingParticipant],
    partySigningKeysWithThreshold: Option[SigningKeysWithThreshold],
) extends TopologyMapping
    with KeyMapping {

  override def companion: PartyToParticipant.type = PartyToParticipant

  def toProto: v30.PartyToParticipant =
    v30.PartyToParticipant(
      party = partyId.toProtoPrimitive,
      threshold = threshold.value,
      participants = participants.map(_.toProto),
      partySigningKeys = partySigningKeysWithThreshold.map(_.toProto),
    )

  override def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyToParticipant(
        toProto
      )
    )

  override def namespace: Namespace = partyId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

  def participantIds: Seq[ParticipantId] = participants.map(_.participantId)

  def partySigningKeys: Set[SigningPublicKey] =
    // Signing keys have been added in 3.4 as Option, previously we didn't have them. So we fall back to an empty Seq.
    partySigningKeysWithThreshold.map(_.keys.forgetNE).getOrElse(Set.empty)

  override def mappedKeys: Set[PublicKey] = partySigningKeys.toSet[PublicKey]

  override def namespaceKeyForSelfAuthorization: Option[SigningPublicKey] =
    partySigningKeys.find(_.fingerprint == partyId.fingerprint)

  /*
   * See topology.proto for the specification of the authorization rules.
   */
  def requiredAuthInternal(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]],
      legacyPermissionUpgradeRequirements: Boolean,
  ): RequiredAuth = {

    val current = this
    val prev =
      previous.flatMap(_.select[TopologyChangeOp.Replace, PartyToParticipant].map(_.mapping))
    val prevConfirmationThreshold = prev.map(_.threshold)
    val prevParticipants = prev.toList.flatMap(_.participants).toSeq
    val prevPartySigningKeys = prev.flatMap(_.partySigningKeysWithThreshold)

    // a change to the confirmation threshold needs to be authorized by the party
    val confirmationThresholdAuth =
      Option.when(!prevConfirmationThreshold.contains(threshold))(
        RequiredNamespaces(partyId)
      )

    // a change to the party's signing keys needs to be authorized by the party and all new signing keys
    val currentSigningKeys = current.partySigningKeysWithThreshold.toList
      .flatMap(_.keys.map(_.fingerprint))
      .toSet
    val previousSigningKeys =
      prevPartySigningKeys.toList.flatMap(_.keys).map(_.fingerprint).toSet
    val addedSigningKeys = currentSigningKeys -- previousSigningKeys
    val partySigningKeysAuth =
      Option.when(current.partySigningKeysWithThreshold != prevPartySigningKeys)(
        RequiredNamespaces(Set(partyId.namespace), extraKeys = addedSigningKeys)
      )

    val currentParticipantIds = current.participants.map(_.participantId).toSet
    val prevParticipantIds = prevParticipants.map(_.participantId).toSet

    // added participants need to be authorized by the party and the respective participants
    val addedParticipants = currentParticipantIds -- prevParticipantIds
    val addedParticipantsAuth =
      Option.when(addedParticipants.nonEmpty)(
        RequiredNamespaces(Set(partyId.namespace) ++ addedParticipants.map(_.namespace))
      )

    // removing a participant requires the authorization of either the party or the participant
    val removedParticipantsAuth = {
      val removedParticipants = prevParticipantIds -- currentParticipantIds
      removedParticipants.map(removed =>
        // a participant can unilaterally unhost a party
        RequiredNamespaces(partyId).or(RequiredNamespaces(removed))
      )
    }

    val currentPermissions = current.participants.map(h => h.participantId -> h.permission).toMap
    val previousPermissions = prevParticipants.map(h => h.participantId -> h.permission).toMap

    // permission upgrades require the authorization of the party and the participant.
    // if legacyPermissionUpgradeRequirements is true, only the the party's authorization is required.
    // permission downgrades require the authorization of the party or the participant.
    val permissionChangesAuth =
      currentParticipantIds.flatMap { pid =>
        previousPermissions.get(pid).zip(currentPermissions.get(pid)).collect {
          case (prevPerm, currPerm) if prevPerm < currPerm =>
            if (legacyPermissionUpgradeRequirements) RequiredNamespaces(partyId)
            else RequiredNamespaces(partyId, pid)
          case (prevPerm, currPerm) if prevPerm > currPerm =>
            RequiredNamespaces(partyId).or(RequiredNamespaces(pid))
        }
      }

    // clearing the onboarding flag can only be done by the participant.
    // setting the flag can only be done by party.
    val onboardingFlagChangesAuth = {
      val currentFlags = current.participants.map(p => p.participantId -> p.onboarding).toMap
      val previousFlags = prevParticipants.map(p => p.participantId -> p.onboarding).toMap

      // detect only changes to the onboarding flags. the required authorization for the addition or removal
      // of a participant is handled above.
      currentFlags.flatMap { case (pid, onboarding) =>
        previousFlags.get(pid).flatMap { prevOnboarding =>
          (prevOnboarding, onboarding) match {
            case (_prev @ true, _curr @ false) => Some(RequiredNamespaces(pid))
            case (_prev @ false, _curr @ true) =>
              Some(RequiredNamespaces(partyId))
            case _ => None
          }
        }
      }
    }

    // the order of how the individual permissions are "and-ed" together matters slightly,
    // because we can optimize nested
    val allAuthorizations =
      confirmationThresholdAuth.toList.toVector ++ partySigningKeysAuth ++
        addedParticipantsAuth ++ onboardingFlagChangesAuth ++ removedParticipantsAuth ++
        permissionChangesAuth
    // all detected individual authorizations are required for the transaction to become authorized
    val combinedAuthO = allAuthorizations.reduceOption(_.and(_))
    // if no change was detected (which would be in the case of a REMOVE), fall back to requiring the following signatures:
    // * the party
    // * all added participants (for safety)
    // * all added signing keys (for safety)
    combinedAuthO.getOrElse(
      RequiredNamespaces(
        Set(partyId.namespace) ++ addedParticipants.map(_.namespace),
        extraKeys = addedSigningKeys,
      )
    )
  }

  def requiredAuthBackwardsCompatible(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    requiredAuthInternal(previous, legacyPermissionUpgradeRequirements = true)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth =
    requiredAuthInternal(previous, legacyPermissionUpgradeRequirements = false)

  override def uniqueKey: MappingHash = PartyToParticipant.uniqueKey(partyId)

}

object PartyToParticipant extends TopologyMappingCompanion {

  val MaxKeys: Int = KeyMapping.MaxKeys

  def create(
      partyId: PartyId,
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
      partySigningKeysWithThreshold: Option[SigningKeysWithThreshold] = None,
  ): Either[String, PartyToParticipant] = {

    // If a participant is listed several times with different permissions, take the one with the higher
    // Needed for backwards compatibility with existing topologies
    val deduplicateParticipantsWithDifferentPermissionsMap =
      participants
        .groupMapReduce(_.participantId)(identity) { case (first, second) =>
          Ordering.by[HostingParticipant, ParticipantPermission](_.permission).max(first, second)
        }

    val deduplicateParticipantsWithDifferentPermissions = participants
      .map(_.participantId)
      .distinct
      .flatMap(deduplicateParticipantsWithDifferentPermissionsMap.get)

    val keysValid = partySigningKeysWithThreshold.traverse_(signingKeysWithThreshold =>
      KeyMapping.validateKeysSizeSet(
        signingKeysWithThreshold.keys,
        MaxKeys,
      )
    )

    for {
      _ <- keysValid
    } yield PartyToParticipant(
      partyId,
      threshold,
      deduplicateParticipantsWithDifferentPermissions,
      partySigningKeysWithThreshold,
    )
  }

  def tryCreate(
      partyId: PartyId,
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
      partySigningKeysWithThreshold: Option[SigningKeysWithThreshold] = None,
  ): PartyToParticipant =
    create(partyId, threshold, participants, partySigningKeysWithThreshold).valueOr(err =>
      throw new IllegalArgumentException(err)
    )

  def uniqueKey(partyId: PartyId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(partyId.toProtoPrimitive))

  override def code: Code = Code.PartyToParticipant

  def fromProtoV30(
      value: v30.PartyToParticipant
  ): ParsingResult[PartyToParticipant] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt("threshold", value.threshold)
      participants <- value.participants.traverse(HostingParticipant.fromProtoV30)
      partySigningKeys <- value.partySigningKeys.traverse(protoValue =>
        SigningKeysWithThreshold.fromProtoV30(protoValue)
      )
      partyToParticipant <- PartyToParticipant
        .create(partyId, threshold, participants, partySigningKeys)
        .leftMap(ProtoDeserializationError.InvariantViolation(None, _))
    } yield partyToParticipant
}

/** Dynamic synchronizer parameter settings for the synchronizer
  *
  * Each synchronizer has a set of parameters that can be changed at runtime. These changes are
  * authorized by the owner of the synchronizer and distributed to all nodes accordingly.
  */
final case class SynchronizerParametersState(
    synchronizerId: SynchronizerId,
    parameters: DynamicSynchronizerParameters,
) extends TopologyMapping {

  override def companion: SynchronizerParametersState.type = SynchronizerParametersState

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SynchronizerParametersState(
        v30.SynchronizerParametersState(
          synchronizerId = synchronizerId.toProtoPrimitive,
          synchronizerParameters = Some(parameters.toProtoV30),
        )
      )
    )

  override def namespace: Namespace = synchronizerId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(synchronizerId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(synchronizerId)

  override def uniqueKey: MappingHash =
    SynchronizerParametersState.uniqueKey(synchronizerId)
}

object SynchronizerParametersState extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(synchronizerId.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.SynchronizerParametersState

  def fromProtoV30(
      value: v30.SynchronizerParametersState
  ): ParsingResult[SynchronizerParametersState] = {
    val v30.SynchronizerParametersState(synchronizerIdP, synchronizerParametersP) = value
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(
        synchronizerIdP,
        "synchronizer_id",
      )
      parameters <- ProtoConverter.parseRequired(
        DynamicSynchronizerParameters.fromProtoV30,
        "synchronizer_parameters",
        synchronizerParametersP,
      )
    } yield SynchronizerParametersState(synchronizerId, parameters)
  }
}

/** Dynamic sequencing parameter settings for the synchronizer
  *
  * Each synchronizer has a set of sequencing parameters that can be changed at runtime. These
  * changes are authorized by the owner of the synchronizer and distributed to all nodes
  * accordingly.
  */
final case class DynamicSequencingParametersState(
    synchronizerId: SynchronizerId,
    parameters: DynamicSequencingParameters,
) extends TopologyMapping {

  override def companion: DynamicSequencingParametersState.type = DynamicSequencingParametersState

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SequencingDynamicParametersState(
        v30.DynamicSequencingParametersState(
          synchronizerId = synchronizerId.toProtoPrimitive,
          sequencingParameters = Some(parameters.toProtoV30),
        )
      )
    )

  override def namespace: Namespace = synchronizerId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(synchronizerId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(synchronizerId)

  override def uniqueKey: MappingHash = SynchronizerParametersState.uniqueKey(synchronizerId)
}

object DynamicSequencingParametersState extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(synchronizerId.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.SequencingDynamicParametersState

  def fromProtoV30(
      value: v30.DynamicSequencingParametersState
  ): ParsingResult[DynamicSequencingParametersState] = {
    val v30.DynamicSequencingParametersState(synchronizerIdP, sequencingParametersP) = value
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
      representativeProtocolVersion <- DynamicSequencingParameters.protocolVersionRepresentativeFor(
        ProtoVersion(30)
      )
      parameters <- sequencingParametersP
        .map(DynamicSequencingParameters.fromProtoV30)
        .getOrElse(Right(DynamicSequencingParameters.default(representativeProtocolVersion)))
    } yield DynamicSequencingParametersState(synchronizerId, parameters)
  }
}

/** Mediator definition for a synchronizer
  *
  * Each synchronizer needs at least one mediator (group), but can have multiple. Mediators can be
  * temporarily turned off by making them observers. This way, they get informed but they don't have
  * to reply.
  */
final case class MediatorSynchronizerState private (
    synchronizerId: SynchronizerId,
    group: MediatorGroupIndex,
    threshold: PositiveInt,
    active: NonEmpty[Seq[MediatorId]],
    observers: Seq[MediatorId],
) extends TopologyMapping {

  override def companion: MediatorSynchronizerState.type = MediatorSynchronizerState

  lazy val allMediatorsInGroup: NonEmpty[Seq[MediatorId]] = active ++ observers

  def toProto: v30.MediatorSynchronizerState =
    v30.MediatorSynchronizerState(
      synchronizerId = synchronizerId.toProtoPrimitive,
      group = group.unwrap,
      threshold = threshold.unwrap,
      active = active.map(_.uid.toProtoPrimitive),
      observers = observers.map(_.uid.toProtoPrimitive),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.MediatorSynchronizerState(
        toProto
      )
    )

  override def namespace: Namespace = synchronizerId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(synchronizerId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(synchronizerId)

  override def uniqueKey: MappingHash = MediatorSynchronizerState.uniqueKey(synchronizerId, group)
}

object MediatorSynchronizerState extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId, group: MediatorGroupIndex): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(synchronizerId.toProtoPrimitive).add(group.unwrap))

  override def code: TopologyMapping.Code = Code.MediatorSynchronizerState

  def create(
      synchronizerId: SynchronizerId,
      group: MediatorGroupIndex,
      threshold: PositiveInt,
      active: Seq[MediatorId],
      observers: Seq[MediatorId],
  ): Either[String, MediatorSynchronizerState] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of mediator synchronizer state higher than number of mediators ${active.length}",
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
      .toRight("mediator synchronizer state requires at least one active mediator")
  } yield MediatorSynchronizerState(synchronizerId, group, threshold, activeNE, observers.distinct)

  def fromProtoV30(
      value: v30.MediatorSynchronizerState
  ): ParsingResult[MediatorSynchronizerState] = {
    val v30.MediatorSynchronizerState(synchronizerIdP, groupP, thresholdP, activeP, observersP) =
      value
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
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
      result <- create(synchronizerId, group, threshold, active, observers).leftMap(
        ProtoDeserializationError.OtherError.apply
      )
    } yield result
  }

}

/** which sequencers are active on the given synchronizer
  *
  * authorization: whoever controls the synchronizer and all the owners of the active or observing
  * sequencers that were not already present in the tx with serial = n - 1 exception: a sequencer
  * can leave the consortium unilaterally as long as there are enough members to reach the threshold
  * UNIQUE(synchronizer_id)
  */
final case class SequencerSynchronizerState private (
    synchronizerId: SynchronizerId,
    threshold: PositiveInt,
    active: NonEmpty[Seq[SequencerId]],
    observers: Seq[SequencerId],
) extends TopologyMapping {

  override def companion: SequencerSynchronizerState.type = SequencerSynchronizerState

  lazy val allSequencers: NonEmpty[Seq[SequencerId]] = active ++ observers

  def toProto: v30.SequencerSynchronizerState =
    v30.SequencerSynchronizerState(
      synchronizerId = synchronizerId.toProtoPrimitive,
      threshold = threshold.unwrap,
      active = active.map(_.uid.toProtoPrimitive),
      observers = observers.map(_.uid.toProtoPrimitive),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SequencerSynchronizerState(
        toProto
      )
    )

  override def namespace: Namespace = synchronizerId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(synchronizerId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(synchronizerId)

  override def uniqueKey: MappingHash = SequencerSynchronizerState.uniqueKey(synchronizerId)
}

object SequencerSynchronizerState extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(synchronizerId.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.SequencerSynchronizerState

  def create(
      synchronizerId: SynchronizerId,
      threshold: PositiveInt,
      active: Seq[SequencerId],
      observers: Seq[SequencerId],
  ): Either[String, SequencerSynchronizerState] = for {
    _ <- Either.cond(
      threshold.unwrap <= active.length,
      (),
      s"threshold ($threshold) of sequencer synchronizer state higher than number of active sequencers ${active.length}",
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
      .toRight("sequencer synchronizer state requires at least one active sequencer")
  } yield SequencerSynchronizerState(synchronizerId, threshold, activeNE, observers.distinct)

  def fromProtoV30(
      value: v30.SequencerSynchronizerState
  ): ParsingResult[SequencerSynchronizerState] = {
    val v30.SequencerSynchronizerState(synchronizerIdP, thresholdP, activeP, observersP) = value
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
      threshold <- ProtoConverter.parsePositiveInt("threshold", thresholdP)
      active <- activeP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "active").map(SequencerId(_))
      )
      observers <- observersP.traverse(
        UniqueIdentifier.fromProtoPrimitive(_, "observers").map(SequencerId(_))
      )
      result <- create(synchronizerId, threshold, active, observers).leftMap(
        ProtoDeserializationError.OtherError.apply
      )
    } yield result
  }

}

// Indicates the beginning of synchronizer upgrade. Only topology transactions related to synchronizer upgrades are permitted
// after this transaction has become effective. Removing this mapping effectively unfreezes the topology state again.
final case class SynchronizerUpgradeAnnouncement(
    successorSynchronizerId: PhysicalSynchronizerId,
    upgradeTime: CantonTimestamp,
) extends TopologyMapping {

  def successor: SynchronizerSuccessor = SynchronizerSuccessor(successorSynchronizerId, upgradeTime)

  override def companion: SynchronizerUpgradeAnnouncement.type = SynchronizerUpgradeAnnouncement

  def toProto: v30.SynchronizerUpgradeAnnouncement =
    v30.SynchronizerUpgradeAnnouncement(
      successorPhysicalSynchronizerId = successorSynchronizerId.toProtoPrimitive,
      upgradeTime = Some(upgradeTime.toProtoTimestamp),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.SynchronizerUpgradeAnnouncement(
        toProto
      )
    )

  override def namespace: Namespace = successorSynchronizerId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(successorSynchronizerId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(
    successorSynchronizerId.logical
  )

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(successorSynchronizerId)

  override def uniqueKey: MappingHash =
    SynchronizerUpgradeAnnouncement.uniqueKey(successorSynchronizerId.logical)
}

object SynchronizerUpgradeAnnouncement extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(synchronizerId.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.SynchronizerUpgradeAnnouncement

  def fromProtoV30(
      value: v30.SynchronizerUpgradeAnnouncement
  ): ParsingResult[SynchronizerUpgradeAnnouncement] =
    for {
      successorSynchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        value.successorPhysicalSynchronizerId,
        "successor_physical_synchronizer_id",
      )
      upgradeTime <- ProtoConverter
        .parseRequired(
          CantonTimestamp.fromProtoTimestamp,
          "upgradeTime",
          value.upgradeTime,
        )
    } yield SynchronizerUpgradeAnnouncement(successorSynchronizerId, upgradeTime)
}

final case class GrpcConnection(
    endpoints: NonEmpty[Seq[Endpoint]],
    transportSecurity: Boolean,
    customTrustCertificates: Option[ByteString],
) {
  def toProtoV30: v30.SequencerConnectionSuccessor.SequencerConnection =
    v30.SequencerConnectionSuccessor.SequencerConnection(
      v30.SequencerConnectionSuccessor.SequencerConnection.ConnectionType.Grpc(
        v30.SequencerConnectionSuccessor.SequencerConnection.Grpc(
          endpoints = endpoints.map(_.toURI(transportSecurity).toString),
          customTrustCertificates = customTrustCertificates,
        )
      )
    )
}

object GrpcConnection {
  def fromProtoV30(
      value: v30.SequencerConnectionSuccessor.SequencerConnection
  ): ParsingResult[GrpcConnection] = for {
    grpc <- value.connectionType.grpc.toRight(FieldNotSet("grpc"))
    uris <- ProtoConverter.parseRequiredNonEmpty(
      (s: String) =>
        UrlValidator
          .validate(s)
          .leftMap(err => ValueDeserializationError("endpoints", err.message)),
      "endpoints",
      grpc.endpoints,
    )
    endpointsAndTls <- Endpoint
      .fromUris(uris)
      .leftMap(err => ValueConversionError("endpoints", err))
    (endpoints, useTls) = endpointsAndTls

  } yield GrpcConnection(endpoints, useTls, grpc.customTrustCertificates)

}

final case class SequencerConnectionSuccessor(
    sequencerId: SequencerId,
    synchronizerId: SynchronizerId,
    connection: GrpcConnection,
) extends TopologyMapping {
  override def companion: TopologyMappingCompanion = SequencerConnectionSuccessor

  override def namespace: Namespace = sequencerId.namespace

  override def maybeUid: Option[UniqueIdentifier] = Some(sequencerId.uid)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(Set(namespace))

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  def toGrpcSequencerConnection(alias: SequencerAlias): GrpcSequencerConnection =
    GrpcSequencerConnection(
      endpoints = connection.endpoints,
      transportSecurity = connection.transportSecurity,
      customTrustCertificates = connection.customTrustCertificates,
      sequencerAlias = alias,
      sequencerId = Some(sequencerId),
    )

  def toProto: v30.SequencerConnectionSuccessor = v30.SequencerConnectionSuccessor(
    sequencerId = sequencerId.toProtoPrimitive,
    synchronizerId = synchronizerId.toProtoPrimitive,
    connection = Some(connection.toProtoV30),
  )

  override def toProtoV30: v30.TopologyMapping = v30.TopologyMapping(
    v30.TopologyMapping.Mapping.SequencerConnectionSuccessor(
      toProto
    )
  )

  override def uniqueKey: MappingHash =
    SequencerConnectionSuccessor.uniqueKey(sequencerId, synchronizerId)
}

object SequencerConnectionSuccessor extends TopologyMappingCompanion {
  override def code: Code = Code.SequencerConnectionSuccessor
  def uniqueKey(sequencerId: SequencerId, synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(
      _.add(sequencerId.uid.toProtoPrimitive).add(synchronizerId.toProtoPrimitive)
    )

  def fromProtoV30(
      value: v30.SequencerConnectionSuccessor
  ): ParsingResult[SequencerConnectionSuccessor] =
    for {
      sequencerId <- SequencerId.fromProtoPrimitive(value.sequencerId, "sequencer_id")
      currentSynchronizer <- SynchronizerId.fromProtoPrimitive(
        value.synchronizerId,
        "synchronizer_id",
      )
      connection <- ProtoConverter.parseRequired(
        GrpcConnection.fromProtoV30,
        "connection",
        value.connection,
      )
    } yield SequencerConnectionSuccessor(
      sequencerId,
      currentSynchronizer,
      connection,
    )
}
