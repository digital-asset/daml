// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v1.RegisterTopologyTransactionResponse.Result.State as ProtoStateV1
import com.digitalasset.canton.protocol.{v1, v4}
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

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV1)

  def toProtoV1: v1.RegisterTopologyTransactionResponse =
    v1.RegisterTopologyTransactionResponse(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId = requestId.unwrap,
      results = results.map(_.toProtoV1),
      domainId = domainId.unwrap.toProtoPrimitive,
    )

  @transient override protected lazy val companionObj: RegisterTopologyTransactionResponse.type =
    RegisterTopologyTransactionResponse
}

object RegisterTopologyTransactionResponse
    extends HasProtocolVersionedCompanion[RegisterTopologyTransactionResponse] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v1.RegisterTopologyTransactionResponse
    )(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
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

  private[messages] def fromProtoV1(
      message: v1.RegisterTopologyTransactionResponse
  ): ParsingResult[RegisterTopologyTransactionResponse] =
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
      results <- message.results.traverse(RegisterTopologyTransactionResponseResult.fromProtoV1)
    } yield RegisterTopologyTransactionResponse(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      results,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(1)))

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

  def toProtoV1: v1.RegisterTopologyTransactionResponse.Result = {
    import RegisterTopologyTransactionResponseResult.*

    def reply(state: v1.RegisterTopologyTransactionResponse.Result.State) =
      v1.RegisterTopologyTransactionResponse.Result(
        state = state,
        errorMessage = "",
      )

    state match {
      case State.Failed => reply(ProtoStateV1.FAILED)
      case State.Rejected => reply(ProtoStateV1.REJECTED)
      case State.Accepted => reply(ProtoStateV1.ACCEPTED)
      case State.Duplicate => reply(ProtoStateV1.DUPLICATE)
      case State.Obsolete => reply(ProtoStateV1.OBSOLETE)
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

  def fromProtoV1(
      result: v1.RegisterTopologyTransactionResponse.Result
  ): ParsingResult[RegisterTopologyTransactionResponseResult] = {
    result.state match {
      case ProtoStateV1.MISSING_STATE =>
        Left(
          ProtoDeserializationError.OtherError(
            "Missing state for v1.RegisterTopologyTransactionResponse.State.Result"
          )
        )
      case ProtoStateV1.FAILED => Right(RegisterTopologyTransactionResponseResult(State.Failed))
      case ProtoStateV1.REJECTED => Right(RegisterTopologyTransactionResponseResult(State.Rejected))
      case ProtoStateV1.ACCEPTED => Right(RegisterTopologyTransactionResponseResult(State.Accepted))
      case ProtoStateV1.DUPLICATE =>
        Right(RegisterTopologyTransactionResponseResult(State.Duplicate))
      case ProtoStateV1.OBSOLETE => Right(RegisterTopologyTransactionResponseResult(State.Obsolete))
      case ProtoStateV1.Unrecognized(unrecognizedValue) =>
        Left(
          ProtoDeserializationError.OtherError(
            s"Unrecognised state for v1.RegisterTopologyTransactionResponse.State.Result: $unrecognizedValue"
          )
        )
    }
  }
}
