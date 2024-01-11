// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.MediatorResponse.InvalidMediatorResponse
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens

import scala.math.Ordering.Implicits.infixOrderingOps

/** Payload of a response sent to the mediator in reaction to a request.
  *
  * @param requestId The unique identifier of the request.
  * @param sender The identity of the sender.
  * @param viewHashO The value of the field "view hash" in the corresponding view message.
  *                  May be empty if the [[localVerdict]] is [[com.digitalasset.canton.protocol.messages.LocalReject.Malformed]].
  *                  Must be empty if the protoVersion is 3 or higher.
  * @param viewPositionO the view position of the underlying view.
  *                      May be empty if the [[localVerdict]] is [[com.digitalasset.canton.protocol.messages.LocalReject.Malformed]].
  *                      Must be empty if the protoVersion is strictly lower than 2.
  * @param localVerdict The participant's verdict on the request's view.
  * @param rootHash The root hash of the request if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                 or [[com.digitalasset.canton.protocol.messages.LocalReject]]. [[scala.None$]] otherwise.
  * @param confirmingParties The non-empty set of confirming parties of the view hosted by the sender if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                          or [[com.digitalasset.canton.protocol.messages.LocalReject]]. Empty otherwise.
  * @param domainId The domain ID over which the request is sent.
  */

/*
This class is a reference example of serialization best practices, demonstrating:
 * handling of object invariants (i.e., the construction of an instance may fail with an exception)

Please consult the team if you intend to change the design of serialization.

Because
 * `fromProtoV0` is private,
 * the class is `sealed abstract`,
then clients cannot create instances with an incorrect `deserializedFrom` field.

Optional parameters are strongly discouraged, as each parameter needs to be consciously set in a production context.
 */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class MediatorResponse private (
    requestId: RequestId,
    sender: ParticipantId,
    viewHashO: Option[ViewHash],
    viewPositionO: Option[ViewPosition],
    localVerdict: LocalVerdict,
    rootHash: Option[RootHash],
    confirmingParties: Set[LfPartyId],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      MediatorResponse.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[MediatorResponse]
    with HasDomainId
    with PrettyPrinting {

  // Private copy method used by the lenses in the companion object
  private def copy(
      requestId: RequestId = requestId,
      sender: ParticipantId = sender,
      viewHashO: Option[ViewHash] = viewHashO,
      viewPositionO: Option[ViewPosition] = viewPositionO,
      localVerdict: LocalVerdict = localVerdict,
      rootHash: Option[RootHash] = rootHash,
      confirmingParties: Set[LfPartyId] = confirmingParties,
      domainId: DomainId = domainId,
  ): MediatorResponse = MediatorResponse(
    requestId,
    sender,
    viewHashO,
    viewPositionO,
    localVerdict,
    rootHash,
    confirmingParties,
    domainId,
  )(representativeProtocolVersion, None)

  // If an object invariant is violated, throw an exception specific to the class.
  // Thus, the exception can be caught during deserialization and translated to a human readable error message.
  localVerdict match {
    case _: Malformed =>
      if (confirmingParties.nonEmpty)
        throw InvalidMediatorResponse("Confirming parties must be empty for verdict Malformed.")
    case _: LocalApprove | _: LocalReject =>
      if (confirmingParties.isEmpty)
        throw InvalidMediatorResponse(
          show"Confirming parties must not be empty for verdict $localVerdict"
        )
      if (rootHash.isEmpty)
        throw InvalidMediatorResponse(show"Root hash must not be empty for verdict $localVerdict")
      if (protoVersion < ProtoVersion(2) && viewHashO.isEmpty)
        throw InvalidMediatorResponse(show"View mash must not be empty for verdict $localVerdict")
      if (protoVersion >= ProtoVersion(2) && viewPositionO.isEmpty)
        throw InvalidMediatorResponse(
          show"View position must not be empty for verdict $localVerdict"
        )
  }

  if (protoVersion >= ProtoVersion(2) && viewHashO.nonEmpty)
    throw InvalidMediatorResponse(
      s"View hash must be empty for protoVersion $protoVersion."
    )

  if (protoVersion < ProtoVersion(2) && viewPositionO.nonEmpty)
    throw InvalidMediatorResponse(
      s"View position must be empty for protoVersion $protoVersion."
    )

  override def signingTimestamp: CantonTimestamp = requestId.unwrap

  protected override def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: MediatorResponse.type = MediatorResponse

  protected def toProtoV0: v0.MediatorResponse =
    v0.MediatorResponse(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      sender = sender.toProtoPrimitive,
      viewHash = viewHashO.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      localVerdict = Some(localVerdict.toProtoV0),
      rootHash = rootHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      confirmingParties = confirmingParties.toList,
      domainId = domainId.toProtoPrimitive,
    )

  protected def toProtoV1: v1.MediatorResponse =
    v1.MediatorResponse(
      requestId = Some(requestId.toProtoPrimitive),
      sender = sender.toProtoPrimitive,
      viewHash = viewHashO.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      localVerdict = Some(localVerdict.toProtoV1),
      rootHash = rootHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      confirmingParties = confirmingParties.toList,
      domainId = domainId.toProtoPrimitive,
    )

  protected def toProtoV2: v2.MediatorResponse =
    v2.MediatorResponse(
      requestId = Some(requestId.toProtoPrimitive),
      sender = sender.toProtoPrimitive,
      viewPosition = viewPositionO.map(_.toProtoV2),
      localVerdict = Some(localVerdict.toProtoV1),
      rootHash = rootHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      confirmingParties = confirmingParties.toList,
      domainId = domainId.toProtoPrimitive,
    )

  override def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.MediatorResponse =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.MediatorResponse(getCryptographicEvidence)

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.MediatorResponse(
      getCryptographicEvidence
    )

  override def hashPurpose: HashPurpose = HashPurpose.MediatorResponseSignature

  override def pretty: Pretty[this.type] =
    prettyOfClass(
      param("sender", _.sender),
      param("localVerdict", _.localVerdict),
      param("confirmingParties", _.confirmingParties),
      param("domainId", _.domainId),
      param("requestId", _.requestId),
      paramIfDefined("viewHash", _.viewHashO),
      paramIfDefined("viewPosition", _.viewPositionO),
      paramIfDefined("rootHash", _.rootHash),
      param("representativeProtocolVersion", _.representativeProtocolVersion),
    )
}

object MediatorResponse extends HasMemoizedProtocolVersionedWrapperCompanion[MediatorResponse] {
  override val name: String = "MediatorResponse"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.MediatorResponse)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.MediatorResponse)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.MediatorResponse)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  final case class InvalidMediatorResponse(msg: String) extends RuntimeException(msg)

  // Variant of "tryCreate" that returns Left(...) instead of throwing an exception.
  // This is for callers who *do not know up front* whether the parameters meet the object invariants.
  //
  // Optional method, feel free to omit it.
  def create(
      requestId: RequestId,
      sender: ParticipantId,
      viewHashO: Option[ViewHash],
      viewPositionO: Option[ViewPosition],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): Either[InvalidMediatorResponse, MediatorResponse] =
    Either.catchOnly[InvalidMediatorResponse](
      tryCreate(
        requestId,
        sender,
        viewHashO,
        viewPositionO,
        localVerdict,
        rootHash,
        confirmingParties,
        domainId,
        protocolVersion,
      )
    )

  // This method is tailored to the case that the caller already knows that the parameters meet the object invariants.
  // Consequently, the method throws an exception on invalid parameters.
  //
  // The "tryCreate" method has the following advantage over the auto-generated "apply" method:
  // - The deserializedFrom field cannot be set; so it cannot be set incorrectly.
  //
  // The method is called "tryCreate" instead of "apply" for two reasons:
  // - to emphasize that this method may throw an exception
  // - to not confuse the Idea compiler by overloading "apply".
  //   (This is not a problem with this particular class, but it has been a problem with other classes.)
  //
  // The "tryCreate" method is optional.
  // Feel free to omit "tryCreate", if the auto-generated "apply" method is good enough.
  def tryCreate(
      requestId: RequestId,
      sender: ParticipantId,
      viewHashO: Option[ViewHash],
      viewPositionO: Option[ViewPosition],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): MediatorResponse =
    MediatorResponse(
      requestId,
      sender,
      viewHashO.filter(_ => protocolVersion < ProtocolVersion.v5),
      viewPositionO.filter(_ => protocolVersion >= ProtocolVersion.v5),
      localVerdict,
      rootHash,
      confirmingParties,
      domainId,
    )(protocolVersionRepresentativeFor(protocolVersion), None)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val requestIdUnsafe: Lens[MediatorResponse, RequestId] = GenLens[MediatorResponse](_.requestId)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val senderUnsafe: Lens[MediatorResponse, ParticipantId] = GenLens[MediatorResponse](_.sender)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val viewHashOUnsafe: Lens[MediatorResponse, Option[ViewHash]] =
    GenLens[MediatorResponse](_.viewHashO)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val viewPositionOUnsafe: Lens[MediatorResponse, Option[ViewPosition]] =
    GenLens[MediatorResponse](_.viewPositionO)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val localVerdictUnsafe: Lens[MediatorResponse, LocalVerdict] =
    GenLens[MediatorResponse](_.localVerdict)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val rootHashUnsafe: Lens[MediatorResponse, Option[RootHash]] =
    GenLens[MediatorResponse](_.rootHash)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val confirmingPartiesUnsafe: Lens[MediatorResponse, Set[LfPartyId]] =
    GenLens[MediatorResponse](_.confirmingParties)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val domainIdUnsafe: Lens[MediatorResponse, DomainId] = GenLens[MediatorResponse](_.domainId)

  private def fromProtoV0(mediatorResponseP: v0.MediatorResponse)(
      bytes: ByteString
  ): ParsingResult[MediatorResponse] = {
    val v0.MediatorResponse(
      requestIdP,
      senderP,
      viewHashP,
      localVerdictP,
      rootHashP,
      confirmingPartiesP,
      domainIdP,
    ) =
      mediatorResponseP
    for {
      requestId <- ProtoConverter
        .required("MediatorResponse.request_id", requestIdP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      sender <- ParticipantId.fromProtoPrimitive(senderP, "MediatorResponse.sender")
      viewHashO <- ViewHash.fromProtoPrimitiveOption(viewHashP)
      localVerdict <- ProtoConverter
        .required("MediatorResponse.local_verdict", localVerdictP)
        .flatMap(LocalVerdict.fromProtoV0)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      confirmingParties <- confirmingPartiesP.traverse(ProtoConverter.parseLfPartyId)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      response <- Either
        .catchOnly[InvalidMediatorResponse](
          MediatorResponse(
            requestId,
            sender,
            viewHashO,
            None,
            localVerdict,
            rootHashO,
            confirmingParties.toSet,
            domainId,
          )(
            supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(0)),
            Some(bytes),
          )
        )
        .leftMap(err => InvariantViolation(err.toString))
    } yield response
  }

  private def fromProtoV1(mediatorResponseP: v1.MediatorResponse)(
      bytes: ByteString
  ): ParsingResult[MediatorResponse] = {
    val v1.MediatorResponse(
      requestIdPO,
      senderP,
      viewHashP,
      localVerdictPO,
      rootHashP,
      confirmingPartiesP,
      domainIdP,
    ) =
      mediatorResponseP
    for {
      requestId <- ProtoConverter
        .required("MediatorResponse.request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      sender <- ParticipantId.fromProtoPrimitive(senderP, "MediatorResponse.sender")
      viewHashO <- ViewHash.fromProtoPrimitiveOption(viewHashP)
      localVerdict <- ProtoConverter
        .required("MediatorResponse.local_verdict", localVerdictPO)
        .flatMap(LocalVerdict.fromProtoV1)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      confirmingParties <- confirmingPartiesP.traverse(ProtoConverter.parseLfPartyId)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      response <- Either
        .catchOnly[InvalidMediatorResponse](
          MediatorResponse(
            requestId,
            sender,
            viewHashO,
            None,
            localVerdict,
            rootHashO,
            confirmingParties.toSet,
            domainId,
          )(
            supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(1)),
            Some(bytes),
          )
        )
        .leftMap(err => InvariantViolation(err.toString))
    } yield response
  }

  private def fromProtoV2(mediatorResponseP: v2.MediatorResponse)(
      bytes: ByteString
  ): ParsingResult[MediatorResponse] = {
    val v2.MediatorResponse(
      requestIdPO,
      senderP,
      localVerdictPO,
      rootHashP,
      confirmingPartiesP,
      domainIdP,
      viewPositionPO,
    ) =
      mediatorResponseP
    for {
      requestId <- ProtoConverter
        .required("MediatorResponse.request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      sender <- ParticipantId.fromProtoPrimitive(senderP, "MediatorResponse.sender")
      localVerdict <- ProtoConverter
        .required("MediatorResponse.local_verdict", localVerdictPO)
        .flatMap(LocalVerdict.fromProtoV1)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      confirmingParties <- confirmingPartiesP.traverse(ProtoConverter.parseLfPartyId)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewPositionO = viewPositionPO.map(ViewPosition.fromProtoV2)
      response <- Either
        .catchOnly[InvalidMediatorResponse](
          MediatorResponse(
            requestId,
            sender,
            None,
            viewPositionO,
            localVerdict,
            rootHashO,
            confirmingParties.toSet,
            domainId,
          )(
            supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(2)),
            Some(bytes),
          )
        )
        .leftMap(err => InvariantViolation(err.toString))
    } yield response
  }

  implicit val mediatorResponseSignedMessageContentCast
      : SignedMessageContentCast[MediatorResponse] =
    SignedMessageContentCast.create[MediatorResponse]("MediatorResponse") {
      case response: MediatorResponse => Some(response)
      case _ => None
    }
}
