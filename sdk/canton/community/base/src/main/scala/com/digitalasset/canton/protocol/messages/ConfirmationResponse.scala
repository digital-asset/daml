// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.ConfirmationResponse.InvalidConfirmationResponse
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.macros.GenLens
import monocle.{Lens, PTraversal, Traversal}

/** Payload of a response sent to the mediator in reaction to a confirmation request.
  *
  * @param viewPositionO the view position of the underlying view.
  *                      May be empty if the [[localVerdict]] is [[com.digitalasset.canton.protocol.LocalRejectError.Malformed]].
  *                      Must be empty if the protoVersion is strictly lower than 2.
  * @param localVerdict The participant's verdict on the request's view.
  * @param confirmingParties   The set of confirming parties. Empty, if the verdict is malformed.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class ConfirmationResponse private (
    viewPositionO: Option[ViewPosition],
    localVerdict: LocalVerdict,
    confirmingParties: Set[LfPartyId],
) extends PrettyPrinting {

  // Private copy method used by the lenses
  private def copy(
      viewPositionO: Option[ViewPosition] = viewPositionO,
      localVerdict: LocalVerdict = localVerdict,
      confirmingParties: Set[LfPartyId] = confirmingParties,
  ): ConfirmationResponse = ConfirmationResponse(
    viewPositionO,
    localVerdict,
    confirmingParties,
  )

  // If an object invariant is violated, throw an exception specific to the class.
  // Thus, the exception can be caught during deserialization and translated to a human-readable error message.
  if (localVerdict.isMalformed) {
    if (confirmingParties.nonEmpty)
      throw InvalidConfirmationResponse("Confirming parties must be empty for verdict Malformed.")
  } else {
    if (confirmingParties.isEmpty)
      throw InvalidConfirmationResponse(
        show"Confirming parties must not be empty for verdict $localVerdict"
      )
    if (viewPositionO.isEmpty)
      throw InvalidConfirmationResponse(
        show"View position must not be empty for verdict $localVerdict"
      )
  }

  private[messages] def toProtoV30: v30.ConfirmationResponse =
    v30.ConfirmationResponse(
      viewPosition = viewPositionO.map(_.toProtoV30),
      localVerdict = Some(localVerdict.toProtoV30),
      confirmingParties = confirmingParties.toList,
    )

  override protected def pretty: Pretty[this.type] =
    prettyOfClass(
      paramIfDefined("viewPosition", _.viewPositionO),
      param("localVerdict", _.localVerdict),
      param("confirmingParties", _.confirmingParties),
    )
}

object ConfirmationResponse {

  final case class InvalidConfirmationResponse(msg: String) extends RuntimeException(msg)

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
      viewPositionO: Option[ViewPosition],
      localVerdict: LocalVerdict,
      confirmingParties: Set[LfPartyId],
  ): ConfirmationResponse =
    ConfirmationResponse(
      viewPositionO,
      localVerdict,
      confirmingParties,
    )

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
  val confirmingPartiesUnsafe: Lens[ConfirmationResponse, Set[LfPartyId]] =
    GenLens[ConfirmationResponse](_.confirmingParties)

  def fromProtoV30(
      confirmationResponseP: v30.ConfirmationResponse
  ): ParsingResult[ConfirmationResponse] = {
    val v30.ConfirmationResponse(
      localVerdictPO,
      confirmingPartiesP,
      viewPositionPO,
    ) =
      confirmationResponseP
    for {
      localVerdict <- ProtoConverter
        .required("ConfirmationResponse.local_verdict", localVerdictPO)
        .flatMap(LocalVerdict.fromProtoV30)
      confirmingParties <- confirmingPartiesP.traverse(
        ProtoConverter.parseLfPartyId(_, "confirming_parties")
      )
      viewPositionO = viewPositionPO.map(ViewPosition.fromProtoV30)
      response <- Either
        .catchOnly[InvalidConfirmationResponse](
          ConfirmationResponse(
            viewPositionO,
            localVerdict,
            confirmingParties.toSet,
          )
        )
        .leftMap(err => InvariantViolation(field = None, error = err.toString))
    } yield response
  }
}

/** Aggregates multiple confirmation responses to be sent to the mediator in reaction to a confirmation request.
  *
  * @param requestId The unique identifier of the request.
  * @param rootHash The root hash of the request.
  * @param synchronizerId The synchronizer ID over which the request is sent.
  * @param sender The identity of the sender.
  * @param responses A list of confirmation responses.
  */
final case class ConfirmationResponses private (
    requestId: RequestId,
    rootHash: RootHash,
    override val synchronizerId: SynchronizerId,
    sender: ParticipantId,
    responses: NonEmpty[Seq[ConfirmationResponse]],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConfirmationResponses.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[ConfirmationResponses]
    with HasSynchronizerId
    with PrettyPrinting {

  // Private copy method used by the lenses
  private def copy(
      requestId: RequestId = requestId,
      rootHash: RootHash = rootHash,
      synchronizerId: SynchronizerId = synchronizerId,
      sender: ParticipantId = sender,
      responses: NonEmpty[Seq[ConfirmationResponse]] = responses,
  ): ConfirmationResponses = ConfirmationResponses(
    requestId,
    rootHash,
    synchronizerId,
    sender,
    responses,
  )(representativeProtocolVersion, None)

  override def signingTimestamp: Option[CantonTimestamp] = Some(requestId.unwrap)

  protected override def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: ConfirmationResponses.type =
    ConfirmationResponses

  protected def toProtoV30: v30.ConfirmationResponses =
    v30.ConfirmationResponses(
      requestId = requestId.toProtoPrimitive,
      rootHash = rootHash.toProtoPrimitive,
      sender = sender.toProtoPrimitive,
      synchronizerId = synchronizerId.toProtoPrimitive,
      responses = responses.map(_.toProtoV30),
    )

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.ConfirmationResponses(
      getCryptographicEvidence
    )

  override protected def pretty: Pretty[this.type] =
    prettyOfClass(
      param("requestId", _.requestId),
      param("rootHash", _.rootHash),
      param("sender", _.sender),
      param("synchronizerId", _.synchronizerId),
      param("responses", _.responses),
    )

}

object ConfirmationResponses extends VersioningCompanionMemoization[ConfirmationResponses] {
  override val name: String = "ConfirmationResponses"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.ConfirmationResponses)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def tryCreate(
      requestId: RequestId,
      rootHash: RootHash,
      synchronizerId: SynchronizerId,
      sender: ParticipantId,
      responses: NonEmpty[Seq[ConfirmationResponse]],
      protocolVersion: ProtocolVersion,
  ): ConfirmationResponses =
    ConfirmationResponses(
      requestId,
      rootHash,
      synchronizerId,
      sender,
      responses,
    )(protocolVersionRepresentativeFor(protocolVersion), None)

  private val responsesUnsafe =
    Lens[ConfirmationResponses, Seq[ConfirmationResponse]](_.responses)(responses =>
      confirmationResponses =>
        confirmationResponses.copy(responses = NonEmptyUtil.fromUnsafe(responses))
    ).andThen(Traversal.fromTraverse[Seq, ConfirmationResponse])

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val requestIdUnsafe: Lens[ConfirmationResponses, RequestId] =
    GenLens[ConfirmationResponses](_.requestId)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val senderUnsafe: Lens[ConfirmationResponses, ParticipantId] =
    GenLens[ConfirmationResponses](_.sender)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val rootHashUnsafe: Lens[ConfirmationResponses, RootHash] =
    GenLens[ConfirmationResponses](_.rootHash)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val viewPositionOUnsafe: PTraversal[ConfirmationResponses, ConfirmationResponses, Option[
    ViewPosition
  ], Option[ViewPosition]] =
    responsesUnsafe.andThen(ConfirmationResponse.viewPositionOUnsafe)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val localVerdictUnsafe
      : PTraversal[ConfirmationResponses, ConfirmationResponses, LocalVerdict, LocalVerdict] =
    responsesUnsafe.andThen(ConfirmationResponse.localVerdictUnsafe)

  /** DO NOT USE IN PRODUCTION, as this does not necessarily check object invariants. */
  @VisibleForTesting
  val confirmingPartiesUnsafe
      : PTraversal[ConfirmationResponses, ConfirmationResponses, Set[LfPartyId], Set[LfPartyId]] =
    responsesUnsafe.andThen(ConfirmationResponse.confirmingPartiesUnsafe)

  private def fromProtoV30(confirmationResponsesP: v30.ConfirmationResponses)(
      bytes: ByteString
  ): ParsingResult[ConfirmationResponses] = {
    val v30.ConfirmationResponses(requestIdP, rootHashP, synchronizerIdP, senderP, responsesP) =
      confirmationResponsesP
    for {
      requestId <- RequestId.fromProtoPrimitive(requestIdP)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
      sender <- ParticipantId.fromProtoPrimitive(senderP, "sender")
      responses <- ProtoConverter.parseRequiredNonEmpty(
        ConfirmationResponse.fromProtoV30,
        "responses",
        responsesP,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ConfirmationResponses(requestId, rootHash, synchronizerId, sender, responses)(
      rpv,
      Some(bytes),
    )

  }

  implicit val confirmationResponsesSignedMessageContentCast
      : SignedMessageContentCast[ConfirmationResponses] =
    SignedMessageContentCast.create[ConfirmationResponses]("ConfirmationResponses") {
      case response: ConfirmationResponses => Some(response)
      case _ => None
    }
}
