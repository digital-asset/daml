// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  OtherError,
  ValueDeserializationError,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfHashSyntax.*
import com.digitalasset.canton.protocol.RefIdentifierSyntax.*
import com.digitalasset.canton.protocol.{
  GlobalKeySerialization,
  LfActionNode,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeFetch,
  LfNodeLookupByKey,
  LfTemplateId,
  LfTransactionVersion,
  RefIdentifierSyntax,
  v0,
  v1,
  v2,
  v3,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{LfChoiceName, LfInterfaceId, LfPackageId, LfPartyId, LfVersioned}
import com.google.protobuf.ByteString

import scala.math.Ordered.orderingToOrdered

/** Summarizes the information that is needed in addition to the other fields of [[ViewParticipantData]] for
  * determining the root action of a view.
  */
sealed trait ActionDescription
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[ActionDescription] {

  /** Whether the root action was a byKey action (exerciseByKey, fetchByKey, lookupByKey) */
  def byKey: Boolean

  /** The node seed for the root action of a view. Empty for fetch and lookupByKey nodes */
  def seedOption: Option[LfHash]

  /** The lf transaction version of the node */
  def version: LfTransactionVersion

  @transient override protected lazy val companionObj: ActionDescription.type =
    ActionDescription

  protected def toProtoDescriptionV0: v0.ActionDescription.Description
  protected def toProtoDescriptionV1: v1.ActionDescription.Description
  protected def toProtoDescriptionV2: v2.ActionDescription.Description
  protected def toProtoDescriptionV3: v3.ActionDescription.Description

  def toProtoV0: v0.ActionDescription =
    v0.ActionDescription(description = toProtoDescriptionV0)

  def toProtoV1: v1.ActionDescription =
    v1.ActionDescription(description = toProtoDescriptionV1)

  def toProtoV2: v2.ActionDescription =
    v2.ActionDescription(description = toProtoDescriptionV2)

  def toProtoV3: v3.ActionDescription =
    v3.ActionDescription(description = toProtoDescriptionV3)

}

object ActionDescription extends HasProtocolVersionedCompanion[ActionDescription] {
  override lazy val name: String = "ActionDescription"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.ActionDescription)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.ActionDescription)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.ActionDescription)(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v6)(v3.ActionDescription)(
      supportedProtoVersion(_)(fromProtoV3),
      _.toProtoV3.toByteString,
    ),
  )

  final case class InvalidActionDescription(message: String)
      extends RuntimeException(message)
      with PrettyPrinting {
    override def pretty: Pretty[InvalidActionDescription] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  def tryFromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
      packagePreference: Set[LfPackageId],
      protocolVersion: ProtocolVersion,
  ): ActionDescription =
    fromLfActionNode(actionNode, seedO, packagePreference, protocolVersion).valueOr(err =>
      throw err
    )

  /** Extracts the action description from an LF node and the optional seed.
    * @param seedO Must be set iff `node` is a [[com.digitalasset.canton.protocol.LfNodeCreate]] or [[com.digitalasset.canton.protocol.LfNodeExercises]].
    */
  def fromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
      packagePreference: Set[LfPackageId],
      protocolVersion: ProtocolVersion,
  ): Either[InvalidActionDescription, ActionDescription] =
    actionNode match {
      case LfNodeCreate(
            contractId,
            _packageName,
            _templateId,
            _arg,
            _agreementText,
            _signatories,
            _stakeholders,
            _key,
            version,
          ) =>
        for {
          seed <- seedO.toRight(InvalidActionDescription("No seed for a Create node given"))
        } yield CreateActionDescription(contractId, seed, version)(
          protocolVersionRepresentativeFor(protocolVersion)
        )

      case LfNodeExercises(
            inputContract,
            _packageName,
            templateId,
            interfaceId,
            choice,
            _consuming,
            actors,
            chosenValue,
            _stakeholders,
            _signatories,
            _choiceObservers,
            _choiceAuthorizers,
            _children,
            exerciseResult,
            _key,
            byKey,
            version,
          ) =>
        for {
          seed <- seedO.toRight(InvalidActionDescription("No seed for an Exercise node given"))
          actionDescription <- ExerciseActionDescription.create(
            inputContract,
            Option.when(protocolVersion >= ProtocolVersion.v5)(templateId),
            choice,
            interfaceId,
            packagePreference,
            chosenValue,
            actors,
            byKey,
            seed,
            version,
            failed = exerciseResult.isEmpty, // absence of exercise result indicates failure
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription

      case LfNodeFetch(
            inputContract,
            _packageName,
            templateId,
            actingParties,
            _signatories,
            _stakeholders,
            _key,
            byKey,
            version,
          ) =>
        for {
          _ <- Either.cond(
            seedO.isEmpty,
            (),
            InvalidActionDescription("No seed should be given for a Fetch node"),
          )
          actors <- Either.cond(
            actingParties.nonEmpty,
            actingParties,
            InvalidActionDescription("Fetch node without acting parties"),
          )
          actionDescription <- FetchActionDescription.create(
            inputContract,
            actors,
            byKey,
            version,
            Option.when(protocolVersion >= ProtocolVersion.v6)(templateId),
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription

      case LfNodeLookupByKey(_, _, keyWithMaintainers, _result, version) =>
        for {
          _ <- Either.cond(
            seedO.isEmpty,
            (),
            InvalidActionDescription("No seed should be given for a LookupByKey node"),
          )
          actionDescription <- LookupByKeyActionDescription.create(
            keyWithMaintainers.globalKey,
            version,
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription
    }

  private def fromCreateProtoV0(
      c: v0.ActionDescription.CreateActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[CreateActionDescription] = {
    val v0.ActionDescription.CreateActionDescription(contractIdP, seedP, versionP) = c
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      version <- lfVersionFromProtoVersioned(versionP)
    } yield CreateActionDescription(contractId, seed, version)(pv)
  }

  private def choiceFromProto(choiceP: String): ParsingResult[LfChoiceName] =
    LfChoiceName
      .fromString(choiceP)
      .leftMap(err => ValueDeserializationError("choice", err))

  private def fromExerciseProtoV0(
      e: v0.ActionDescription.ExerciseActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[ExerciseActionDescription] = {
    val v0.ActionDescription
      .ExerciseActionDescription(
        inputContractIdP,
        choiceP,
        chosenValueB,
        actorsP,
        byKey,
        seedP,
        versionP,
        failed,
      ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      choice <- choiceFromProto(choiceP)
      interfaceId = None
      version <- lfVersionFromProtoVersioned(versionP)
      chosenValue <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          templateId = None,
          choice,
          interfaceId,
          packagePreference = Set.empty,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
          pv,
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromExerciseProtoV1(
      e: v1.ActionDescription.ExerciseActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[ExerciseActionDescription] = {
    val v1.ActionDescription
      .ExerciseActionDescription(
        inputContractIdP,
        choiceP,
        chosenValueB,
        actorsP,
        byKey,
        seedP,
        versionP,
        failed,
        interfaceIdP,
      ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      choice <- choiceFromProto(choiceP)
      interfaceId <- interfaceIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      version <- lfVersionFromProtoVersioned(versionP)
      chosenValue <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          templateId = None,
          choice,
          interfaceId,
          packagePreference = Set.empty,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
          pv,
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromExerciseProtoV2(
      e: v2.ActionDescription.ExerciseActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[ExerciseActionDescription] = {
    val v2.ActionDescription.ExerciseActionDescription(
      inputContractIdP,
      choiceP,
      chosenValueB,
      actorsP,
      byKey,
      seedP,
      versionP,
      failed,
      interfaceIdP,
      templateIdP,
    ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      templateId <- templateIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      choice <- choiceFromProto(choiceP)
      interfaceId <- interfaceIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      version <- lfVersionFromProtoVersioned(versionP)
      chosenValue <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          templateId,
          choice,
          interfaceId,
          packagePreference = Set.empty,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
          pv,
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromExerciseProtoV3(
      e: v3.ActionDescription.ExerciseActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[ExerciseActionDescription] = {
    val v3.ActionDescription.ExerciseActionDescription(
      inputContractIdP,
      choiceP,
      chosenValueB,
      actorsP,
      byKey,
      seedP,
      versionP,
      failed,
      interfaceIdP,
      templateIdP,
      packagePreferenceP,
    ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      templateId <- templateIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      packagePreference <- packagePreferenceP.traverse(ProtoConverter.parsePackageId).map(_.toSet)
      choice <- choiceFromProto(choiceP)
      interfaceId <- interfaceIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      version <- lfVersionFromProtoVersioned(versionP)
      chosenValue <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          templateId,
          choice,
          interfaceId,
          packagePreference,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
          pv,
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromLookupByKeyProtoV0(
      k: v0.ActionDescription.LookupByKeyActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[LookupByKeyActionDescription] = {
    val v0.ActionDescription.LookupByKeyActionDescription(keyP) = k
    for {
      key <- ProtoConverter
        .required("key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      actionDescription <- LookupByKeyActionDescription
        .create(key.unversioned, key.version, pv)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromLookupByKeyProtoV1(
      k: v1.ActionDescription.LookupByKeyActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[LookupByKeyActionDescription] = {
    val v1.ActionDescription.LookupByKeyActionDescription(keyP) = k
    for {
      key <- ProtoConverter
        .required("key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV1)
      actionDescription <- LookupByKeyActionDescription
        .create(key.unversioned, key.version, pv)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromFetchProtoV0(
      f: v0.ActionDescription.FetchActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[FetchActionDescription] = {
    val v0.ActionDescription.FetchActionDescription(inputContractIdP, actorsP, byKey, versionP) = f
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      version <- lfVersionFromProtoVersioned(versionP)
      actionDescription <- FetchActionDescription
        .create(inputContractId, actors, byKey, version, None, pv)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromFetchProtoV1(
      f: v1.ActionDescription.FetchActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[FetchActionDescription] = {
    val v1.ActionDescription.FetchActionDescription(
      inputContractIdP,
      actorsP,
      byKey,
      versionP,
      templateIdP,
    ) = f
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      version <- lfVersionFromProtoVersioned(versionP)
      templateId <- templateIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      actionDescription <- FetchActionDescription
        .create(inputContractId, actors, byKey, version, templateId, pv)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private[data] def fromProtoV0(
      actionDescriptionP: v0.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v0.ActionDescription.Description.*
    val v0.ActionDescription(description) = actionDescriptionP

    protocolVersionRepresentativeFor(ProtoVersion(0)).flatMap { pv =>
      description match {
        case Create(create) => fromCreateProtoV0(create, pv)
        case Exercise(exercise) => fromExerciseProtoV0(exercise, pv)
        case Fetch(fetch) => fromFetchProtoV0(fetch, pv)
        case LookupByKey(lookup) => fromLookupByKeyProtoV0(lookup, pv)
        case Empty => Left(FieldNotSet("description"))
      }
    }
  }

  private[data] def fromProtoV1(
      actionDescriptionP: v1.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v1.ActionDescription.Description.*
    val v1.ActionDescription(description) = actionDescriptionP

    protocolVersionRepresentativeFor(ProtoVersion(1)).flatMap { pv =>
      description match {
        case Create(create) => fromCreateProtoV0(create, pv)
        case Exercise(exercise) => fromExerciseProtoV1(exercise, pv)
        case Fetch(fetch) => fromFetchProtoV0(fetch, pv)
        case LookupByKey(lookup) => fromLookupByKeyProtoV0(lookup, pv)
        case Empty => Left(FieldNotSet("description"))
      }
    }
  }

  private[data] def fromProtoV2(
      actionDescriptionP: v2.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v2.ActionDescription.Description.*
    val v2.ActionDescription(description) = actionDescriptionP

    protocolVersionRepresentativeFor(ProtoVersion(2)).flatMap { pv =>
      description match {
        case Create(create) => fromCreateProtoV0(create, pv)
        case Exercise(exercise) => fromExerciseProtoV2(exercise, pv)
        case Fetch(fetch) => fromFetchProtoV0(fetch, pv)
        case LookupByKey(lookup) => fromLookupByKeyProtoV0(lookup, pv)
        case Empty => Left(FieldNotSet("description"))
      }
    }

  }

  private[data] def fromProtoV3(
      actionDescriptionP: v3.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v3.ActionDescription.Description.*
    val v3.ActionDescription(description) = actionDescriptionP

    protocolVersionRepresentativeFor(ProtoVersion(3)).flatMap { pv =>
      description match {
        case Create(create) => fromCreateProtoV0(create, pv)
        case Exercise(exercise) => fromExerciseProtoV3(exercise, pv)
        case Fetch(fetch) => fromFetchProtoV1(fetch, pv)
        case LookupByKey(lookup) => fromLookupByKeyProtoV1(lookup, pv)
        case Empty => Left(FieldNotSet("description"))
      }
    }

  }

  private def lfVersionFromProtoVersioned(
      versionP: String
  ): ParsingResult[LfTransactionVersion] = TransactionVersion.All
    .find(_.protoValue == versionP)
    .toRight(s"Unsupported transaction version $versionP")
    .leftMap(ValueDeserializationError("version", _))

  def serializeChosenValue(
      chosenValue: Value,
      transactionVersion: LfTransactionVersion,
  ): Either[String, ByteString] =
    ValueCoder
      .encodeValue(ValueCoder.CidEncoder, transactionVersion, chosenValue)
      .leftMap(_.errorMessage)

  final case class CreateActionDescription(
      contractId: LfContractId,
      seed: LfHash,
      override val version: LfTransactionVersion,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {
    override def byKey: Boolean = false

    override def seedOption: Option[LfHash] = Some(seed)

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.Create =
      v0.ActionDescription.Description.Create(
        v0.ActionDescription.CreateActionDescription(
          contractId = contractId.toProtoPrimitive,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.Create =
      v1.ActionDescription.Description.Create(toProtoDescriptionV0.value)

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.Create =
      v2.ActionDescription.Description.Create(toProtoDescriptionV0.value)

    override protected def toProtoDescriptionV3: v3.ActionDescription.Description.Create =
      v3.ActionDescription.Description.Create(toProtoDescriptionV0.value)

    override def pretty: Pretty[CreateActionDescription] = prettyOfClass(
      param("contract Id", _.contractId),
      param("seed", _.seed),
      param("version", _.version),
    )
  }

  /** @throws InvalidActionDescription if the `chosen_value` cannot be serialized */
  final case class ExerciseActionDescription private (
      inputContractId: LfContractId,
      templateId: Option[LfTemplateId],
      choice: LfChoiceName,
      interfaceId: Option[LfInterfaceId],
      packagePreference: Set[LfPackageId],
      chosenValue: Value,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      seed: LfHash,
      override val version: LfTransactionVersion,
      failed: Boolean,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {

    private val serializedChosenValue: ByteString = serializeChosenValue(chosenValue, version)
      .valueOr(err => throw InvalidActionDescription(s"Failed to serialize chosen value: $err"))

    override def seedOption: Option[LfHash] = Some(seed)

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.Exercise =
      v0.ActionDescription.Description.Exercise(
        v0.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          choice = choice,
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
          failed = failed,
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.Exercise =
      v1.ActionDescription.Description.Exercise(
        v1.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          choice = choice,
          interfaceId = interfaceId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
          failed = failed,
        )
      )

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.Exercise =
      v2.ActionDescription.Description.Exercise(
        v2.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          templateId = templateId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          choice = choice,
          interfaceId = interfaceId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
          failed = failed,
        )
      )

    override protected def toProtoDescriptionV3: v3.ActionDescription.Description.Exercise =
      v3.ActionDescription.Description.Exercise(
        v3.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          templateId = templateId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          packagePreference = packagePreference.toSeq,
          choice = choice,
          interfaceId = interfaceId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
          failed = failed,
        )
      )

    override def pretty: Pretty[ExerciseActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("template id", _.templateId),
      param("choice", _.choice.unquoted),
      param("chosen value", _.chosenValue),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      param("seed", _.seed),
      param("version", _.version),
      paramIfTrue("failed", _.failed),
    )
  }

  object ExerciseActionDescription {
    private[data] val interfaceSupportedSince
        : RepresentativeProtocolVersion[ActionDescription.type] =
      protocolVersionRepresentativeFor(ProtocolVersion.v4)
    private[data] val templateIdSupportedSince
        : RepresentativeProtocolVersion[ActionDescription.type] =
      protocolVersionRepresentativeFor(ProtocolVersion.v5)
    private[data] val packagePreferenceSupportedSince
        : RepresentativeProtocolVersion[ActionDescription.type] =
      protocolVersionRepresentativeFor(ProtocolVersion.v6)

    def tryCreate(
        inputContractId: LfContractId,
        templateId: Option[LfTemplateId],
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
        packagePreference: Set[LfPackageId],
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): ExerciseActionDescription = create(
      inputContractId,
      templateId,
      choice,
      interfaceId,
      packagePreference,
      chosenValue,
      actors,
      byKey,
      seed,
      version,
      failed,
      protocolVersion,
    ).fold(err => throw err, identity)

    def create(
        inputContractId: LfContractId,
        templateId: Option[LfTemplateId],
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
        packagePreference: Set[LfPackageId],
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): Either[InvalidActionDescription, ExerciseActionDescription] = {
      val canHaveInterface = protocolVersion >= interfaceSupportedSince
      val canHaveTemplateId = protocolVersion >= templateIdSupportedSince

      for {
        _ <- Either.cond(
          interfaceId.isEmpty || canHaveInterface,
          (),
          InvalidActionDescription(
            s"Protocol version is equivalent to ${protocolVersion.representative} but interface id is supported since protocol version $interfaceSupportedSince"
          ),
        )

        _ <- Either.cond(
          templateId.isEmpty || canHaveTemplateId,
          (),
          InvalidActionDescription(
            s"Protocol version is equivalent to ${protocolVersion.representative} but template id is supported since protocol version $templateIdSupportedSince"
          ),
        )

        action <- Either.catchOnly[InvalidActionDescription](
          ExerciseActionDescription(
            inputContractId,
            templateId,
            choice,
            interfaceId,
            packagePreference,
            chosenValue,
            actors,
            byKey,
            seed,
            version,
            failed,
          )(protocolVersion)
        )
      } yield action
    }
  }

  final case class FetchActionDescription private (
      inputContractId: LfContractId,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      override val version: LfTransactionVersion,
      templateId: Option[LfTemplateId],
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription
      with NoCopy {

    override def seedOption: Option[LfHash] = None

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.Fetch =
      v0.ActionDescription.Description.Fetch(
        v0.ActionDescription.FetchActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          actors = actors.toSeq,
          byKey = byKey,
          version = version.protoValue,
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.Fetch =
      v1.ActionDescription.Description.Fetch(toProtoDescriptionV0.value)

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.Fetch =
      v2.ActionDescription.Description.Fetch(toProtoDescriptionV0.value)

    override protected def toProtoDescriptionV3: v3.ActionDescription.Description.Fetch =
      v3.ActionDescription.Description.Fetch(
        v1.ActionDescription.FetchActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          actors = actors.toSeq,
          byKey = byKey,
          version = version.protoValue,
          templateId = templateId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
        )
      )

    override def pretty: Pretty[FetchActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      param("version", _.version),
    )
  }

  object FetchActionDescription {

    private[data] val templateIdSupportedSince
        : RepresentativeProtocolVersion[ActionDescription.type] =
      protocolVersionRepresentativeFor(ProtocolVersion.v6)

    def tryCreate(
        inputContractId: LfContractId,
        actors: Set[LfPartyId],
        byKey: Boolean,
        version: LfTransactionVersion,
        templateId: Option[LfTemplateId],
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): FetchActionDescription = {
      create(inputContractId, actors, byKey, version, templateId, protocolVersion).valueOr(err =>
        throw err
      )
    }

    def create(
        inputContractId: LfContractId,
        actors: Set[LfPartyId],
        byKey: Boolean,
        version: LfTransactionVersion,
        templateId: Option[LfTemplateId],
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): Either[InvalidActionDescription, FetchActionDescription] = {

      val canHaveTemplateId = protocolVersion >= templateIdSupportedSince

      for {
        _ <- Either.cond(
          templateId.isEmpty || canHaveTemplateId,
          (),
          InvalidActionDescription(
            s"Protocol version is equivalent to ${protocolVersion.representative} but template id is supported since protocol version $templateIdSupportedSince"
          ),
        )
      } yield new FetchActionDescription(inputContractId, actors, byKey, version, templateId)(
        protocolVersion
      )
    }

  }

  final case class LookupByKeyActionDescription private (
      key: LfGlobalKey,
      override val version: LfTransactionVersion,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {

    override def byKey: Boolean = true

    override def seedOption: Option[LfHash] = None

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.LookupByKey =
      v0.ActionDescription.Description.LookupByKey(
        v0.ActionDescription.LookupByKeyActionDescription(
          key = Some(GlobalKeySerialization.assertToProtoV0(LfVersioned(version, key)))
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.LookupByKey =
      v1.ActionDescription.Description.LookupByKey(toProtoDescriptionV0.value)

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.LookupByKey =
      v2.ActionDescription.Description.LookupByKey(toProtoDescriptionV0.value)

    override protected def toProtoDescriptionV3: v3.ActionDescription.Description.LookupByKey =
      v3.ActionDescription.Description.LookupByKey(
        v1.ActionDescription.LookupByKeyActionDescription(
          key = Some(GlobalKeySerialization.assertToProtoV1(LfVersioned(version, key)))
        )
      )

    override def pretty: Pretty[LookupByKeyActionDescription] = prettyOfClass(
      param("key", _.key),
      param("version", _.version),
    )
  }

  object LookupByKeyActionDescription {
    def tryCreate(
        key: LfGlobalKey,
        version: LfTransactionVersion,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): LookupByKeyActionDescription =
      new LookupByKeyActionDescription(key, version)(protocolVersion)

    def create(
        key: LfGlobalKey,
        version: LfTransactionVersion,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): Either[InvalidActionDescription, LookupByKeyActionDescription] =
      Either.catchOnly[InvalidActionDescription](tryCreate(key, version, protocolVersion))

  }
}
