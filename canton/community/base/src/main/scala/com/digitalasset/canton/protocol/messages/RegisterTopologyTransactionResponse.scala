// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v30.RegisterTopologyTransactionResponse.Result.State as ProtoStateV1
import com.digitalasset.canton.protocol.{messages, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

final case class RegisterTopologyTransactionResponse(
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    results: Seq[RegisterTopologyTransactionResponseResult],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      RegisterTopologyTransactionResponse.type
    ]
) extends UnsignedProtocolMessage {

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV30)

  def toProtoV30: v30.RegisterTopologyTransactionResponse =
    v30.RegisterTopologyTransactionResponse(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId = requestId.unwrap,
      results = results.map(_.toProtoV30),
      domainId = domainId.unwrap.toProtoPrimitive,
    )

  @transient override protected lazy val companionObj: RegisterTopologyTransactionResponse.type =
    RegisterTopologyTransactionResponse
}

object RegisterTopologyTransactionResponse
    extends HasProtocolVersionedCompanion[RegisterTopologyTransactionResponse] {
  val supportedProtoVersions: messages.RegisterTopologyTransactionResponse.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
        v30.RegisterTopologyTransactionResponse
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  def apply(
      requestedBy: Member,
      participant: ParticipantId,
      requestId: TopologyRequestId,
      results: Seq[RegisterTopologyTransactionResponseResult],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): RegisterTopologyTransactionResponse =
    RegisterTopologyTransactionResponse(requestedBy, participant, requestId, results, domainId)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  private[messages] def fromProtoV30(
      message: v30.RegisterTopologyTransactionResponse
  ): ParsingResult[RegisterTopologyTransactionResponse] =
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
      results <- message.results.traverse(RegisterTopologyTransactionResponseResult.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield RegisterTopologyTransactionResponse(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      results,
      DomainId(domainUid),
    )(rpv)

  override def name: String = "RegisterTopologyTransactionResponse"

  def create(
      request: RegisterTopologyTransactionRequest,
      results: List[RegisterTopologyTransactionResponseResult],
      protocolVersion: ProtocolVersion,
  ): RegisterTopologyTransactionResponse =
    RegisterTopologyTransactionResponse(
      request.requestedBy,
      request.participant,
      request.requestId,
      results,
      request.domainId,
    )(RegisterTopologyTransactionResponse.protocolVersionRepresentativeFor(protocolVersion))

  implicit val registerTopologyTransactionResponseCast: ProtocolMessageContentCast[
    RegisterTopologyTransactionResponse
  ] =
    ProtocolMessageContentCast
      .create[RegisterTopologyTransactionResponse](
        "RegisterTopologyTransactionResponse"
      ) {
        case rttr: RegisterTopologyTransactionResponse => Some(rttr)
        case _ => None
      }
}

final case class RegisterTopologyTransactionResponseResult(
    state: RegisterTopologyTransactionResponseResult.State
) extends PrettyPrinting {

  def toProtoV30: v30.RegisterTopologyTransactionResponse.Result = {
    import RegisterTopologyTransactionResponseResult.*

    def reply(state: v30.RegisterTopologyTransactionResponse.Result.State) =
      v30.RegisterTopologyTransactionResponse.Result(
        state = state,
        errorMessage = "",
      )

    state match {
      case State.Failed => reply(ProtoStateV1.STATE_FAILED)
      case State.Rejected => reply(ProtoStateV1.STATE_REJECTED)
      case State.Accepted => reply(ProtoStateV1.STATE_ACCEPTED)
      case State.Duplicate => reply(ProtoStateV1.STATE_DUPLICATE)
      case State.Obsolete => reply(ProtoStateV1.STATE_OBSOLETE)
    }
  }

  override def pretty: Pretty[RegisterTopologyTransactionResponseResult] = prettyOfClass(
    param("state", _.state)
  )
}

object RegisterTopologyTransactionResponseResult {
  sealed trait State extends Product with Serializable with PrettyPrinting {
    override def pretty: Pretty[this.type] = prettyOfObject[this.type]
  }

  object State {
    case object Failed extends State

    case object Rejected extends State

    case object Accepted extends State

    case object Duplicate extends State

    /** Unnecessary removes are marked as obsolete */
    case object Obsolete extends State
  }

  def fromProtoV30(
      result: v30.RegisterTopologyTransactionResponse.Result
  ): ParsingResult[RegisterTopologyTransactionResponseResult] = {
    result.state match {
      case ProtoStateV1.STATE_UNSPECIFIED =>
        Left(
          ProtoDeserializationError.OtherError(
            "Missing state for v1.RegisterTopologyTransactionResponse.State.Result"
          )
        )
      case ProtoStateV1.STATE_FAILED =>
        Right(RegisterTopologyTransactionResponseResult(State.Failed))
      case ProtoStateV1.STATE_REJECTED =>
        Right(RegisterTopologyTransactionResponseResult(State.Rejected))
      case ProtoStateV1.STATE_ACCEPTED =>
        Right(RegisterTopologyTransactionResponseResult(State.Accepted))
      case ProtoStateV1.STATE_DUPLICATE =>
        Right(RegisterTopologyTransactionResponseResult(State.Duplicate))
      case ProtoStateV1.STATE_OBSOLETE =>
        Right(RegisterTopologyTransactionResponseResult(State.Obsolete))
      case ProtoStateV1.Unrecognized(unrecognizedValue) =>
        Left(
          ProtoDeserializationError.OtherError(
            s"Unrecognised state for v1.RegisterTopologyTransactionResponse.State.Result: $unrecognizedValue"
          )
        )
    }
  }
}
