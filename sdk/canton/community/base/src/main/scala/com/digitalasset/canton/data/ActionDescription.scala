// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
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
  v30,
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
import com.digitalasset.canton.{LfChoiceName, LfInterfaceId, LfPackageId, LfPartyId}
import com.google.protobuf.ByteString

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

  @transient override protected lazy val companionObj: ActionDescription.type =
    ActionDescription

  protected def toProtoDescriptionV30: v30.ActionDescription.Description

  def toProtoV30: v30.ActionDescription =
    v30.ActionDescription(description = toProtoDescriptionV30)
}

object ActionDescription extends HasProtocolVersionedCompanion[ActionDescription] {
  override lazy val name: String = "ActionDescription"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.ActionDescription)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
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
        } yield CreateActionDescription(contractId, seed)(
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
            Some(templateId),
            choice,
            interfaceId,
            packagePreference,
            chosenValue,
            actors,
            byKey,
            seed,
            failed = exerciseResult.isEmpty, // absence of exercise result indicates failure
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription

      case LfNodeFetch(
            inputContract,
            _packageName,
            _templateId,
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
        } yield FetchActionDescription(inputContract, actors, byKey)(
          protocolVersionRepresentativeFor(protocolVersion)
        )

      case LfNodeLookupByKey(_, _, keyWithMaintainers, _result, version) =>
        for {
          _ <- Either.cond(
            seedO.isEmpty,
            (),
            InvalidActionDescription("No seed should be given for a LookupByKey node"),
          )
          actionDescription <- LookupByKeyActionDescription.create(
            keyWithMaintainers.globalKey,
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription
    }

  private def fromCreateProtoV30(
      c: v30.ActionDescription.CreateActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[CreateActionDescription] = {
    val v30.ActionDescription.CreateActionDescription(contractIdP, seedP) = c
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
    } yield CreateActionDescription(contractId, seed)(pv)
  }

  private def choiceFromProto(choiceP: String): ParsingResult[LfChoiceName] =
    LfChoiceName
      .fromString(choiceP)
      .leftMap(err => ValueDeserializationError("choice", err))

  private def fromExerciseProtoV30(
      e: v30.ActionDescription.ExerciseActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[ExerciseActionDescription] = {
    val v30.ActionDescription.ExerciseActionDescription(
      inputContractIdP,
      choiceP,
      chosenValueB,
      actorsP,
      byKey,
      seedP,
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
      chosenValue <- ValueCoder
        .decodeValue(LfTransactionVersion.maxVersion, chosenValueB)
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
          failed,
          pv,
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromLookupByKeyProtoV30(
      k: v30.ActionDescription.LookupByKeyActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[LookupByKeyActionDescription] = {
    val v30.ActionDescription.LookupByKeyActionDescription(keyP) = k
    for {
      key <- ProtoConverter
        .required("key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV30)
      actionDescription <- LookupByKeyActionDescription
        .create(key, pv)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromFetchProtoV30(
      f: v30.ActionDescription.FetchActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[FetchActionDescription] = {
    val v30.ActionDescription.FetchActionDescription(inputContractIdP, actorsP, byKey) = f
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
    } yield FetchActionDescription(inputContractId, actors, byKey)(pv)
  }

  private[data] def fromProtoV30(
      actionDescriptionP: v30.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v30.ActionDescription.Description.*
    val v30.ActionDescription(description) = actionDescriptionP

    val pv = protocolVersionRepresentativeFor(ProtoVersion(30))

    description match {
      case Create(create) => pv.flatMap(fromCreateProtoV30(create, _))
      case Exercise(exercise) => pv.flatMap(fromExerciseProtoV30(exercise, _))
      case Fetch(fetch) => pv.flatMap(fromFetchProtoV30(fetch, _))
      case LookupByKey(lookup) => pv.flatMap(fromLookupByKeyProtoV30(lookup, _))
      case Empty => Left(FieldNotSet("description"))
    }
  }

  def serializeChosenValue(
      chosenValue: Value
  ): Either[String, ByteString] =
    ValueCoder
      .encodeValue(valueVersion = LfTransactionVersion.maxVersion, v0 = chosenValue)
      .leftMap(_.errorMessage)

  final case class CreateActionDescription(
      contractId: LfContractId,
      seed: LfHash,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {
    override def byKey: Boolean = false

    override def seedOption: Option[LfHash] = Some(seed)

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.Create =
      v30.ActionDescription.Description.Create(
        v30.ActionDescription.CreateActionDescription(
          contractId = contractId.toProtoPrimitive,
          nodeSeed = seed.toProtoPrimitive,
        )
      )

    override def pretty: Pretty[CreateActionDescription] = prettyOfClass(
      param("contract Id", _.contractId),
      param("seed", _.seed),
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
      failed: Boolean,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {

    private val serializedChosenValue: ByteString = serializeChosenValue(chosenValue)
      .valueOr(err => throw InvalidActionDescription(s"Failed to serialize chosen value: $err"))

    override def seedOption: Option[LfHash] = Some(seed)

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.Exercise =
      v30.ActionDescription.Description.Exercise(
        v30.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          templateId = templateId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          packagePreference = packagePreference.toSeq,
          choice = choice,
          interfaceId = interfaceId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
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
      paramIfTrue("failed", _.failed),
    )
  }

  object ExerciseActionDescription {
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
        failed: Boolean,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): Either[InvalidActionDescription, ExerciseActionDescription] =
      Either.catchOnly[InvalidActionDescription](
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
          failed,
        )(protocolVersion)
      )

  }

  final case class FetchActionDescription(
      inputContractId: LfContractId,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription
      with NoCopy {

    override def seedOption: Option[LfHash] = None

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.Fetch =
      v30.ActionDescription.Description.Fetch(
        v30.ActionDescription.FetchActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          actors = actors.toSeq,
          byKey = byKey,
        )
      )

    override def pretty: Pretty[FetchActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
    )
  }

  final case class LookupByKeyActionDescription private (
      key: LfGlobalKey
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {

    private val serializedKey =
      GlobalKeySerialization
        .toProto(key)
        .valueOr(err => throw InvalidActionDescription(s"Failed to serialize key: $err"))

    override def byKey: Boolean = true

    override def seedOption: Option[LfHash] = None

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.LookupByKey =
      v30.ActionDescription.Description.LookupByKey(
        v30.ActionDescription.LookupByKeyActionDescription(
          key = Some(serializedKey)
        )
      )

    override def pretty: Pretty[LookupByKeyActionDescription] = prettyOfClass(
      param("key", _.key)
    )
  }

  object LookupByKeyActionDescription {
    def tryCreate(
        key: LfGlobalKey,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): LookupByKeyActionDescription =
      new LookupByKeyActionDescription(key)(protocolVersion)

    def create(
        key: LfGlobalKey,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): Either[InvalidActionDescription, LookupByKeyActionDescription] =
      Either.catchOnly[InvalidActionDescription](tryCreate(key, protocolVersion))

  }
}
