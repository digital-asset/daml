// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.ConfirmationResponse.InvalidConfirmationResponse
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens

/** Payload of a response sent to the mediator in reaction to a confirmation request.
  *
  * @param requestId The unique identifier of the request.
  * @param sender The identity of the sender.
  * @param viewPositionO the view position of the underlying view.
  *                      May be empty if the [[localVerdict]] is [[com.digitalasset.canton.protocol.LocalRejectError.Malformed]].
  *                      Must be empty if the protoVersion is strictly lower than 2.
  * @param localVerdict The participant's verdict on the request's view.
  * @param rootHash The root hash of the request if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                 or [[com.digitalasset.canton.protocol.messages.LocalReject]]. [[scala.None$]] otherwise.
  * @param confirmingParties   The non-empty set of confirming parties of the view hosted by the sender if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
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
case class ConfirmationResponse private (
    requestId: RequestId,
    sender: ParticipantId,
    viewPositionO: Option[ViewPosition],
    localVerdict: LocalVerdict,
    rootHash: Option[RootHash],
    confirmingParties: Set[LfPartyId],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConfirmationResponse.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[ConfirmationResponse]
    with HasDomainId
    with PrettyPrinting {

  // Private copy method used by the lenses in the companion object
  private def copy(
      requestId: RequestId = requestId,
      sender: ParticipantId = sender,
      viewPositionO: Option[ViewPosition] = viewPositionO,
      localVerdict: LocalVerdict = localVerdict,
      rootHash: Option[RootHash] = rootHash,
      confirmingParties: Set[LfPartyId] = confirmingParties,
      domainId: DomainId = domainId,
  ): ConfirmationResponse = ConfirmationResponse(
    requestId,
    sender,
    viewPositionO,
    localVerdict,
    rootHash,
    confirmingParties,
    domainId,
  )(representativeProtocolVersion, None)

  // If an object invariant is violated, throw an exception specific to the class.
  // Thus, the exception can be caught during deserialization and translated to a human readable error message.
  if (localVerdict.isMalformed) {
    if (confirmingParties.nonEmpty)
      throw InvalidConfirmationResponse("Confirming parties must be empty for verdict Malformed.")
  } else {
    if (confirmingParties.isEmpty)
      throw InvalidConfirmationResponse(
        show"Confirming parties must not be empty for verdict $localVerdict"
      )
    if (rootHash.isEmpty)
      throw InvalidConfirmationResponse(
        show"Root hash must not be empty for verdict $localVerdict"
      )
    if (viewPositionO.isEmpty)
      throw InvalidConfirmationResponse(
        show"View position must not be empty for verdict $localVerdict"
      )
  }

  override def signingTimestamp: Option[CantonTimestamp] = Some(requestId.unwrap)

  protected override def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: ConfirmationResponse.type =
    ConfirmationResponse

  protected def toProtoV30: v30.ConfirmationResponse =
    v30.ConfirmationResponse(
      requestId = requestId.toProtoPrimitive,
      sender = sender.toProtoPrimitive,
      viewPosition = viewPositionO.map(_.toProtoV30),
      localVerdict = Some(localVerdict.toProtoV30),
      rootHash = rootHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      confirmingParties = confirmingParties.toList,
      domainId = domainId.toProtoPrimitive,
    )

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.ConfirmationResponse(
      getCryptographicEvidence
    )

  override def pretty: Pretty[this.type] =
    prettyOfClass(
      param("sender", _.sender),
      param("localVerdict", _.localVerdict),
      param("confirmingParties", _.confirmingParties),
      param("domainId", _.domainId),
      param("requestId", _.requestId),
      paramIfDefined("viewPosition", _.viewPositionO),
      paramIfDefined("rootHash", _.rootHash),
      param("representativeProtocolVersion", _.representativeProtocolVersion),
    )
}

object ConfirmationResponse
    extends HasMemoizedProtocolVersionedWrapperCompanion[ConfirmationResponse] {
  override val name: String = "ConfirmationResponse"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.ConfirmationResponse)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  final case class InvalidConfirmationResponse(msg: String) extends RuntimeException(msg)

  // Variant of "tryCreate" that returns Left(...) instead of throwing an exception.
  // This is for callers who *do not know up front* whether the parameters meet the object invariants.
  //
  // Optional method, feel free to omit it.
  def create(
      requestId: RequestId,
      sender: ParticipantId,
      viewPositionO: Option[ViewPosition],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): Either[InvalidConfirmationResponse, ConfirmationResponse] =
    Either.catchOnly[InvalidConfirmationResponse](
      tryCreate(
        requestId,
        sender,
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
      viewPositionO: Option[ViewPosition],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): ConfirmationResponse =
    ConfirmationResponse(
      requestId,
      sender,
      viewPositionO,
      localVerdict,
      rootHash,
      confirmingParties,
      domainId,
    )(protocolVersionRepresentativeFor(protocolVersion), None)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val requestIdUnsafe: Lens[ConfirmationResponse, RequestId] =
    GenLens[ConfirmationResponse](_.requestId)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val senderUnsafe: Lens[ConfirmationResponse, ParticipantId] =
    GenLens[ConfirmationResponse](_.sender)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val viewPositionOUnsafe: Lens[ConfirmationResponse, Option[ViewPosition]] =
    GenLens[ConfirmationResponse](_.viewPositionO)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val localVerdictUnsafe: Lens[ConfirmationResponse, LocalVerdict] =
    GenLens[ConfirmationResponse](_.localVerdict)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val rootHashUnsafe: Lens[ConfirmationResponse, Option[RootHash]] =
    GenLens[ConfirmationResponse](_.rootHash)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val confirmingPartiesUnsafe: Lens[ConfirmationResponse, Set[LfPartyId]] =
    GenLens[ConfirmationResponse](_.confirmingParties)

  private def fromProtoV30(confirmationResponseP: v30.ConfirmationResponse)(
      bytes: ByteString
  ): ParsingResult[ConfirmationResponse] = {
    val v30.ConfirmationResponse(
      requestIdP,
      senderP,
      localVerdictPO,
      rootHashP,
      confirmingPartiesP,
      domainIdP,
      viewPositionPO,
    ) =
      confirmationResponseP
    for {
      requestId <- RequestId.fromProtoPrimitive(requestIdP)
      sender <- ParticipantId.fromProtoPrimitive(senderP, "ConfirmationResponse.sender")
      localVerdict <- ProtoConverter
        .required("ConfirmationResponse.local_verdict", localVerdictPO)
        .flatMap(LocalVerdict.fromProtoV30)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      confirmingParties <- confirmingPartiesP.traverse(ProtoConverter.parseLfPartyId)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewPositionO = viewPositionPO.map(ViewPosition.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      response <- Either
        .catchOnly[InvalidConfirmationResponse](
          ConfirmationResponse(
            requestId,
            sender,
            viewPositionO,
            localVerdict,
            rootHashO,
            confirmingParties.toSet,
            domainId,
          )(rpv, Some(bytes))
        )
        .leftMap(err => InvariantViolation(err.toString))
    } yield response
  }

  implicit val confirmationResponseSignedMessageContentCast
      : SignedMessageContentCast[ConfirmationResponse] =
    SignedMessageContentCast.create[ConfirmationResponse]("ConfirmationResponse") {
      case response: ConfirmationResponse => Some(response)
      case _ => None
    }
}
