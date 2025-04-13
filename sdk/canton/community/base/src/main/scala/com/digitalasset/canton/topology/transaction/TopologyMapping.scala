// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.protocol.{
  DynamicSequencingParameters,
  DynamicSynchronizerParameters,
  v30,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.RequiredAuth.*
import com.digitalasset.canton.topology.transaction.TopologyMapping.{
  Code,
  MappingHash,
  RequiredAuth,
}
import com.digitalasset.canton.version.ProtoVersion
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
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
    case object IdentifierDelegation
        extends Code("idd", v30Code.TOPOLOGY_MAPPING_CODE_IDENTIFIER_DELEGATION)
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

    // reserved Code(10), was AuthorityOf

    case object SynchronizerParametersState
        extends Code("dop", v30Code.TOPOLOGY_MAPPING_CODE_SYNCHRONIZER_PARAMETERS_STATE)
    case object MediatorSynchronizerState
        extends Code("mds", v30Code.TOPOLOGY_MAPPING_CODE_MEDIATOR_SYNCHRONIZER_STATE)
    case object SequencerSynchronizerState
        extends Code("sds", v30Code.TOPOLOGY_MAPPING_CODE_SEQUENCER_SYNCHRONIZER_STATE)

    // reserved Code(14), was OffboardParticipant

    case object PurgeTopologyTransaction
        extends Code("ptt", v30Code.TOPOLOGY_MAPPING_CODE_PURGE_TOPOLOGY_TXS)
    // Don't reuse It was the TrafficControlState code mapping
    case object SequencingDynamicParametersState
        extends Code("sep", v30Code.TOPOLOGY_MAPPING_CODE_SEQUENCING_DYNAMIC_PARAMETERS_STATE)
    case object PartyToKeyMapping
        extends Code("ptk", v30Code.TOPOLOGY_MAPPING_CODE_PARTY_TO_KEY_MAPPING)

    lazy val all: Seq[Code] = Seq(
      NamespaceDelegation,
      IdentifierDelegation,
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
      PurgeTopologyTransaction,
      SequencingDynamicParametersState,
      PartyToKeyMapping,
    )

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
      uids: Set[UniqueIdentifier] = Set.empty,
      extraKeys: Set[Fingerprint] = Set.empty,
  ) extends PrettyPrinting {
    def isEmpty: Boolean =
      namespaces.isEmpty && uids.isEmpty && extraKeys.isEmpty

    override protected def pretty: Pretty[ReferencedAuthorizations.this.type] = prettyOfClass(
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
            namespaces = x.namespaces ++ y.namespaces,
            uids = x.uids ++ y.uids,
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

    /** Authorizations referenced by this instance. Note that the result is not equivalent to this
      * instance, as an "or" gets translated to an "and". Instead, the result indicates which
      * authorization keys need to be evaluated in order to check if this RequiredAuth is met.
      */
    def referenced: ReferencedAuthorizations
  }

  object RequiredAuth {

    final case class RequiredNamespaces(
        namespaces: Set[Namespace]
    ) extends RequiredAuth {
      override def satisfiedByActualAuthorizers(
          provided: ReferencedAuthorizations
      ): Either[ReferencedAuthorizations, Unit] = {
        val missing = namespaces.filter(ns => !provided.namespaces(ns))
        Either.cond(
          missing.isEmpty,
          (),
          ReferencedAuthorizations(namespaces = missing),
        )
      }

      override def referenced: ReferencedAuthorizations =
        ReferencedAuthorizations(namespaces = namespaces)

      override protected def pretty: Pretty[RequiredNamespaces.this.type] = prettyOfClass(
        unnamedParam(_.namespaces)
      )
    }

    /** Required authorizations for a set of UIDs
      *
      * @param extraKeys
      *   any additional key that needs to authorize the topology transaction, used to verify that
      *   an owner really owns the key
      */
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
      case Mapping.PurgeTopologyTxs(value) => PurgeTopologyTransaction.fromProtoV30(value)
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

/** Represents a restriction of namespace or identifier delegations to specific mapping types.
  */
sealed trait DelegationRestriction extends Product with Serializable {
  def canSign(mappingToSign: Code): Boolean
  def toProtoV30: Seq[v30.Enums.TopologyMappingCode]
}
object DelegationRestriction {

  /** If no mapping restrictions are specified, returns CanSignAllMappings.
    */
  def fromProtoV30(
      mappingRestrictions: Seq[v30.Enums.TopologyMappingCode]
  ): ParsingResult[DelegationRestriction] =
    for {
      mappingRestrictions <- mappingRestrictions.traverse(Code.fromProtoV30)
    } yield NonEmpty.from(mappingRestrictions.toSet) match {
      case None => CanSignAllMappings
      case Some(mappingRestrictionsNE) =>
        CanSignSpecificMappings(mappingRestrictionsNE)
    }

  /** Indicates that there are no mapping restrictions and is represented by no restrictions being
    * specified in the proto message. The target key of the delegation will also be permitted to
    * sign topology mappings that are added in future releases.
    */
  case object CanSignAllMappings extends DelegationRestriction {
    override def canSign(mappingToSign: Code) = true

    override def toProtoV30: Seq[v30.Enums.TopologyMappingCode] =
      // this delegation restriction will be represented as `restrict_to_mappings = [], is_root_delegation = true`
      Seq.empty
  }

  /** Indicates that the key can be used to sign all mappings except for namespace delegations. The
    * target key of the delegation will also be permitted to sign topology mappings that are added
    * in future releases.
    */
  case object CanSignAllButNamespaceDelegations extends DelegationRestriction {
    override def canSign(mappingToSign: Code): Boolean = mappingToSign != Code.NamespaceDelegation

    override def toProtoV30: Seq[Enums.TopologyMappingCode] =
      // this delegation restriction will be represented as `restrict_to_mappings = [], is_root_delegation = false`
      Seq.empty
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

    override def toProtoV30: Seq[v30.Enums.TopologyMappingCode] =
      mappings.map(_.toProtoV30).toSeq.sortBy(_.value)
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
    restrictedToMappings: DelegationRestriction,
) extends TopologyMapping {

  override def companion: NamespaceDelegation.type = NamespaceDelegation

  def toProto: v30.NamespaceDelegation =
    v30.NamespaceDelegation(
      namespace = namespace.fingerprint.unwrap,
      targetKey = Some(target.toProtoV30),
      // only set the isRootDelegation flag for `CanSignAllMappings`
      isRootDelegation = restrictedToMappings == CanSignAllMappings,
      restrictedToMappings = restrictedToMappings.toProtoV30,
    )

  def canSign(mappingsToSign: Code): Boolean =
    restrictedToMappings.canSign(mappingsToSign)

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
}

object NamespaceDelegation extends TopologyMappingCompanion {

  def uniqueKey(namespace: Namespace, target: Fingerprint): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(namespace.fingerprint.unwrap).add(target.unwrap))

  def create(
      namespace: Namespace,
      target: SigningPublicKey,
      restrictedToMappings: DelegationRestriction,
  ): Either[String, NamespaceDelegation] =
    for {
      // The key must have `Namespace` listed as a usage to be eligible as the target of a namespace delegation.
      _ <- Either.cond(
        SigningKeyUsage.matchesRelevantUsages(target.usage, SigningKeyUsage.NamespaceOnly),
        (),
        s"The key ${target.id} must include a ${SigningKeyUsage.Namespace} usage.",
      )
      namespaceDelegation <- Either.cond(
        restrictedToMappings.canSign(Code.NamespaceDelegation) ||
          namespace.fingerprint != target.fingerprint,
        NamespaceDelegation(namespace, target, restrictedToMappings),
        s"Root certificate for $namespace needs to be be able to sign other NamespaceDelegations",
      )

    } yield namespaceDelegation

  @VisibleForTesting
  private[canton] def tryCreate(
      namespace: Namespace,
      target: SigningPublicKey,
      restrictedToMappings: DelegationRestriction,
  ): NamespaceDelegation =
    create(namespace, target, restrictedToMappings).valueOr(err =>
      throw new IllegalArgumentException((err))
    )

  override def code: TopologyMapping.Code = Code.NamespaceDelegation

  /** Returns true if the given transaction is a self-signed root certificate */
  def isRootCertificate(sit: GenericSignedTopologyTransaction): Boolean =
    sit.mapping
      .select[transaction.NamespaceDelegation]
      .exists(ns =>
        // a root certificate must only be signed by the namespace key, but we accept multiple signatures from that key
        sit.signatures.forall(_.signedBy == ns.namespace.fingerprint) &&
          // explicitly checking for nonEmpty to guard against refactorings away from NonEmpty[Set[...]].
          sit.signatures.nonEmpty &&
          ns.canSign(Code.NamespaceDelegation) &&
          ns.target.fingerprint == ns.namespace.fingerprint
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
      restriction <-
        if (value.restrictedToMappings.isEmpty) {
          // this branch is for maintaining backwards compatibility
          ParsingResult.pure[DelegationRestriction](
            if (value.isRootDelegation) CanSignAllMappings
            else CanSignAllButNamespaceDelegations
          )
        } else {
          for {
            _ <- Either.cond(
              !value.isRootDelegation,
              (),
              // if restricted_to_mappings is non empty, then is_root_delegation must not be set to true
              ProtoDeserializationError.InvariantViolation(
                "is_root_delegation",
                "is_root_delegation was set to true, but the list of mapping restrictions did not contain NamespaceDelegation",
              ),
            )
            restriction <- DelegationRestriction.fromProtoV30(value.restrictedToMappings)
          } yield restriction
        }
      namespaceDelegation <- NamespaceDelegation
        .create(namespace, target, restriction)
        .leftMap(err => ProtoDeserializationError.InvariantViolation(None, err))

    } yield namespaceDelegation

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

/** An identifier delegation
  *
  * entrusts a public-key to do any change with respect to the identifier {(X,I) => p_k}
  */
final case class IdentifierDelegation private (
    identifier: UniqueIdentifier,
    target: SigningPublicKey,
) extends TopologyMapping {

  override def companion: IdentifierDelegation.type = IdentifierDelegation

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

  override def namespace: Namespace = identifier.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(identifier)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredNamespaces(Set(namespace))

  override def uniqueKey: MappingHash =
    IdentifierDelegation.uniqueKey(identifier, target.fingerprint)
}

object IdentifierDelegation extends TopologyMappingCompanion {

  def create(
      identifier: UniqueIdentifier,
      target: SigningPublicKey,
  ): Either[String, IdentifierDelegation] =
    // The key must have `IdentityDelegation` listed as a usage to be eligible as the target of an identifier delegation.
    Either.cond(
      SigningKeyUsage.matchesRelevantUsages(target.usage, SigningKeyUsage.IdentityDelegationOnly),
      IdentifierDelegation(identifier, target),
      s"The key ${target.id} must include a ${SigningKeyUsage.IdentityDelegation} usage.",
    )

  protected[canton] def tryCreate(
      identifier: UniqueIdentifier,
      target: SigningPublicKey,
  ): IdentifierDelegation =
    create(identifier, target).valueOr(err => throw new IllegalArgumentException(err))

  def uniqueKey(identifier: UniqueIdentifier, targetKey: Fingerprint): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(identifier.toProtoPrimitive).add(targetKey.unwrap))

  override def code: Code = Code.IdentifierDelegation

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
      identifierDelegation <- create(identifier, target).leftMap(
        InvariantViolation(None, _)
      )
    } yield identifierDelegation
}

/** A topology mapping that maps to a set of public keys for which ownership has to be proven. */
sealed trait KeyMapping extends Product with Serializable {
  def mappedKeys: NonEmpty[Seq[PublicKey]]
}

/** A key owner (participant, mediator, sequencer) to key mapping
  *
  * In Canton, we need to know keys for all participating entities. The entities are all the
  * protocol members (participant, mediator) plus the sequencer (which provides the communication
  * infrastructure for the protocol members).
  */
final case class OwnerToKeyMapping(
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

  override def mappedKeys: NonEmpty[Seq[PublicKey]] = keys
}

object OwnerToKeyMapping extends TopologyMappingCompanion {

  def uniqueKey(member: Member): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(member.uid.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.OwnerToKeyMapping

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
  * In Canton, we can delegate the submission authorisation to a participant node, or we can sign
  * the transaction outside of the node with a party key. This mapping is used to map the party to a
  * set of public keys authorized to sign submissions.
  */
final case class PartyToKeyMapping private (
    party: PartyId,
    threshold: PositiveInt,
    signingKeys: NonEmpty[Seq[SigningPublicKey]],
) extends TopologyMapping
    with KeyMapping {

  override def companion: PartyToKeyMapping.type = PartyToKeyMapping

  def toProto: v30.PartyToKeyMapping = v30.PartyToKeyMapping(
    party = party.toProtoPrimitive,
    threshold = threshold.unwrap,
    signingKeys = signingKeys.map(_.toProtoV30),
  )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PartyToKeyMapping(
        toProto
      )
    )

  override def namespace: Namespace = party.namespace

  override def maybeUid: Option[UniqueIdentifier] = Some(party.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

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

  override def uniqueKey: MappingHash = PartyToKeyMapping.uniqueKey(party)

  override def mappedKeys: NonEmpty[Seq[PublicKey]] = signingKeys.toSeq
}

object PartyToKeyMapping extends TopologyMappingCompanion {

  def create(
      partyId: PartyId,
      threshold: PositiveInt,
      signingKeys: NonEmpty[Seq[SigningPublicKey]],
  ): Either[String, PartyToKeyMapping] = {
    val noDuplicateKeys = {
      val duplicateKeys = signingKeys.groupBy(_.fingerprint).values.filter(_.sizeIs > 1).toList
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
        .map(_ => PartyToKeyMapping(partyId, threshold, signingKeys))

    noDuplicateKeys.flatMap(_ => thresholdCanBeMet)
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
    } yield PartyToKeyMapping(party, threshold, signingKeysNE)
  }

}

/** Participant synchronizer trust certificate
  */
final case class SynchronizerTrustCertificate(
    participantId: ParticipantId,
    synchronizerId: SynchronizerId,
) extends TopologyMapping {

  override def companion: SynchronizerTrustCertificate.type = SynchronizerTrustCertificate

  def toProto: v30.SynchronizerTrustCertificate =
    v30.SynchronizerTrustCertificate(
      participantUid = participantId.uid.toProtoPrimitive,
      synchronizerId = synchronizerId.toProtoPrimitive,
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
    RequiredUids(Set(participantId.uid))

  override def uniqueKey: MappingHash =
    SynchronizerTrustCertificate.uniqueKey(participantId, synchronizerId)
}

object SynchronizerTrustCertificate extends TopologyMappingCompanion {

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
    } yield SynchronizerTrustCertificate(
      participantId,
      synchronizerId,
    )
}

/** Permissions of a participant, i.e., things a participant can do on behalf of a party
  *
  * Permissions are hierarchical. A participant who can submit can confirm. A participant who can
  * confirm can observe.
  */
sealed trait ParticipantPermission extends Product with Serializable {
  def toProtoV30: v30.Enums.ParticipantPermission
  def canConfirm: Boolean
}
object ParticipantPermission {
  case object Submission extends ParticipantPermission {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
    def canConfirm: Boolean = true
  }
  case object Confirmation extends ParticipantPermission {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
    def canConfirm: Boolean = true
  }
  case object Observation extends ParticipantPermission {
    lazy val toProtoV30: Enums.ParticipantPermission =
      v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
    def canConfirm: Boolean = false
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

  def toParticipantAttributes: ParticipantAttributes =
    ParticipantAttributes(permission, loginAfter)

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
    RequiredUids(Set(synchronizerId.uid))

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
    RequiredUids(Set(synchronizerId.uid))

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
  * @param validFrom
  *   optional inclusive start of the validity period in LET
  * @param validUntil
  *   optional exclusive end of the validity period in LET
  *
  * Note that as validFrom and validUntil are in ledger effective time, the boundaries have
  * different semantics from topology transaction validity boundaries.
  */
final case class VettedPackage(
    packageId: LfPackageId,
    validFrom: Option[CantonTimestamp],
    validUntil: Option[CantonTimestamp],
) extends PrettyPrinting {

  def validAt(ts: CantonTimestamp): Boolean = validFrom.forall(_ <= ts) && validUntil.forall(_ > ts)

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
    RequiredUids(Set(participantId.uid))

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

  override def companion: PartyToParticipant.type = PartyToParticipant

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

  override def namespace: Namespace = partyId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(partyId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = None

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

object PartyToParticipant extends TopologyMappingCompanion {

  def create(
      partyId: PartyId,
      threshold: PositiveInt,
      participants: Seq[HostingParticipant],
  ): Either[String, PartyToParticipant] = {
    val noDuplicatePParticipants = {
      val duplicatePermissions =
        participants.groupBy(_.participantId).values.filter(_.sizeIs > 1).toList
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

  override def code: Code = Code.PartyToParticipant

  def fromProtoV30(
      value: v30.PartyToParticipant
  ): ParsingResult[PartyToParticipant] =
    for {
      partyId <- PartyId.fromProtoPrimitive(value.party, "party")
      threshold <- ProtoConverter.parsePositiveInt("threshold", value.threshold)
      participants <- value.participants.traverse(HostingParticipant.fromProtoV30)
    } yield PartyToParticipant(partyId, threshold, participants)
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
  ): RequiredAuth = RequiredUids(Set(synchronizerId.uid))

  override def uniqueKey: MappingHash = SynchronizerParametersState.uniqueKey(synchronizerId)
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
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
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
  ): RequiredAuth = RequiredUids(Set(synchronizerId.uid))

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
  ): RequiredAuth = RequiredUids(Set(synchronizerId.uid))

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
  ): RequiredAuth = RequiredUids(Set(synchronizerId.uid))

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

// Purge topology transaction
final case class PurgeTopologyTransaction private (
    synchronizerId: SynchronizerId,
    mappings: NonEmpty[Seq[TopologyMapping]],
) extends TopologyMapping {

  override def companion: PurgeTopologyTransaction.type = PurgeTopologyTransaction

  def toProto: v30.PurgeTopologyTransaction =
    v30.PurgeTopologyTransaction(
      synchronizerId = synchronizerId.toProtoPrimitive,
      mappings = mappings.map(_.toProtoV30),
    )

  def toProtoV30: v30.TopologyMapping =
    v30.TopologyMapping(
      v30.TopologyMapping.Mapping.PurgeTopologyTxs(
        toProto
      )
    )

  override def namespace: Namespace = synchronizerId.namespace
  override def maybeUid: Option[UniqueIdentifier] = Some(synchronizerId.uid)

  override def restrictedToSynchronizer: Option[SynchronizerId] = Some(synchronizerId)

  override def requiredAuth(
      previous: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): RequiredAuth = RequiredUids(Set(synchronizerId.uid))

  override def uniqueKey: MappingHash = PurgeTopologyTransaction.uniqueKey(synchronizerId)
}

object PurgeTopologyTransaction extends TopologyMappingCompanion {

  def uniqueKey(synchronizerId: SynchronizerId): MappingHash =
    TopologyMapping.buildUniqueKey(code)(_.add(synchronizerId.toProtoPrimitive))

  override def code: TopologyMapping.Code = Code.PurgeTopologyTransaction

  def create(
      synchronizerId: SynchronizerId,
      mappings: Seq[TopologyMapping],
  ): Either[String, PurgeTopologyTransaction] = for {
    mappingsToPurge <- NonEmpty
      .from(mappings)
      .toRight("purge topology transaction requires at least one topology mapping")
  } yield PurgeTopologyTransaction(synchronizerId, mappingsToPurge)

  def fromProtoV30(
      value: v30.PurgeTopologyTransaction
  ): ParsingResult[PurgeTopologyTransaction] = {
    val v30.PurgeTopologyTransaction(synchronizerIdP, mappingsP) = value
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
      mappings <- mappingsP.traverse(TopologyMapping.fromProtoV30)
      result <- create(synchronizerId, mappings).leftMap(
        ProtoDeserializationError.OtherError.apply
      )
    } yield result
  }

}
