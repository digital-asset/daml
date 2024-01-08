// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  v2,
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
import com.digitalasset.canton.{LfChoiceName, LfInterfaceId, LfPartyId, LfVersioned}
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

  /** The lf transaction version of the node */
  def version: LfTransactionVersion

  @transient override protected lazy val companionObj: ActionDescription.type =
    ActionDescription

  protected def toProtoDescriptionV2: v2.ActionDescription.Description

  def toProtoV2: v2.ActionDescription =
    v2.ActionDescription(description = toProtoDescriptionV2)

}

object ActionDescription extends HasProtocolVersionedCompanion[ActionDescription] {
  override lazy val name: String = "ActionDescription"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v30)(v2.ActionDescription)(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
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
      protocolVersion: ProtocolVersion,
  ): ActionDescription =
    fromLfActionNode(actionNode, seedO, protocolVersion).valueOr(err => throw err)

  /** Extracts the action description from an LF node and the optional seed.
    * @param seedO Must be set iff `node` is a [[com.digitalasset.canton.protocol.LfNodeCreate]] or [[com.digitalasset.canton.protocol.LfNodeExercises]].
    */
  def fromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
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
            Some(templateId),
            choice,
            interfaceId,
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
        } yield FetchActionDescription(inputContract, actors, byKey, version)(
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
            version,
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription
    }

  private def fromCreateProtoV2(
      c: v2.ActionDescription.CreateActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[CreateActionDescription] = {
    val v2.ActionDescription.CreateActionDescription(contractIdP, seedP, versionP) = c
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

  private def fromLookupByKeyProtoV2(
      k: v2.ActionDescription.LookupByKeyActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[LookupByKeyActionDescription] = {
    val v2.ActionDescription.LookupByKeyActionDescription(keyP) = k
    for {
      key <- ProtoConverter
        .required("key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      actionDescription <- LookupByKeyActionDescription
        .create(key.unversioned, key.version, pv)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromFetchProtoV2(
      f: v2.ActionDescription.FetchActionDescription,
      pv: RepresentativeProtocolVersion[ActionDescription.type],
  ): ParsingResult[FetchActionDescription] = {
    val v2.ActionDescription.FetchActionDescription(inputContractIdP, actorsP, byKey, versionP) = f
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      version <- lfVersionFromProtoVersioned(versionP)
    } yield FetchActionDescription(inputContractId, actors, byKey, version)(pv)
  }

  private[data] def fromProtoV2(
      actionDescriptionP: v2.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v2.ActionDescription.Description.*
    val v2.ActionDescription(description) = actionDescriptionP

    val pv = protocolVersionRepresentativeFor(ProtoVersion(2))

    description match {
      case Create(create) => fromCreateProtoV2(create, pv)
      case Exercise(exercise) => fromExerciseProtoV2(exercise, pv)
      case Fetch(fetch) => fromFetchProtoV2(fetch, pv)
      case LookupByKey(lookup) => fromLookupByKeyProtoV2(lookup, pv)
      case Empty => Left(FieldNotSet("description"))
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

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.Create =
      v2.ActionDescription.Description.Create(
        v2.ActionDescription.CreateActionDescription(
          contractId = contractId.toProtoPrimitive,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
        )
      )

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
    def tryCreate(
        inputContractId: LfContractId,
        templateId: Option[LfTemplateId],
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
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
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription.type],
    ): Either[InvalidActionDescription, ExerciseActionDescription] =
      Either.catchOnly[InvalidActionDescription](
        ExerciseActionDescription(
          inputContractId,
          templateId,
          choice,
          interfaceId,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
        )(protocolVersion)
      )

  }

  final case class FetchActionDescription(
      inputContractId: LfContractId,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      override val version: LfTransactionVersion,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription
      with NoCopy {

    override def seedOption: Option[LfHash] = None

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.Fetch =
      v2.ActionDescription.Description.Fetch(
        v2.ActionDescription.FetchActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          actors = actors.toSeq,
          byKey = byKey,
          version = version.protoValue,
        )
      )

    override def pretty: Pretty[FetchActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      param("version", _.version),
    )
  }

  final case class LookupByKeyActionDescription private (
      key: LfGlobalKey,
      override val version: LfTransactionVersion,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        ActionDescription.type
      ]
  ) extends ActionDescription {

    private val serializedKey =
      GlobalKeySerialization
        .toProto(LfVersioned(version, key))
        .valueOr(err => throw InvalidActionDescription(s"Failed to serialize key: $err"))

    override def byKey: Boolean = true

    override def seedOption: Option[LfHash] = None

    override protected def toProtoDescriptionV2: v2.ActionDescription.Description.LookupByKey =
      v2.ActionDescription.Description.LookupByKey(
        v2.ActionDescription.LookupByKeyActionDescription(
          key = Some(serializedKey)
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
