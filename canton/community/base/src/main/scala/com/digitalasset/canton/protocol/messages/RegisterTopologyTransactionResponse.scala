// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v0.RegisterTopologyTransactionResponse.Result.State as ProtoStateV0
import com.digitalasset.canton.protocol.v1.RegisterTopologyTransactionResponse.Result.State as ProtoStateV1
import com.digitalasset.canton.protocol.{v0, v1, v2, v3, v4}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

final case class RegisterTopologyTransactionResponse[
    +Res <: RegisterTopologyTransactionResponseResult
](
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    results: Seq[Res],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      RegisterTopologyTransactionResponse.type
    ]
) extends UnsignedProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with ProtocolMessageV3
    with UnsignedProtocolMessageV4 {

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV1)
    )

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(
      v2.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV1)
    )

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(
      v3.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV1)
    )

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV1)

  def toProtoV0: v0.RegisterTopologyTransactionResponse =
    v0.RegisterTopologyTransactionResponse(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId = requestId.unwrap,
      results = results.map(_.toProtoV0),
      domainId = domainId.unwrap.toProtoPrimitive,
    )

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
    extends HasProtocolVersionedCompanion[
      RegisterTopologyTransactionResponse[RegisterTopologyTransactionResponseResult]
    ] {
  import RegisterTopologyTransactionResponseResult.*

  type Result = RegisterTopologyTransactionResponse[RegisterTopologyTransactionResponseResult]

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.RegisterTopologyTransactionResponse
    )(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(
      v1.RegisterTopologyTransactionResponse
    )(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  sealed trait Error {
    def message: String
  }
  case object ResultVersionsMixture extends Error {
    val message =
      "Cannot create a RegisterTopologyTransactionResponse containing a mixture of Result.V0 and Result.V1"
  }

  def apply[Res <: RegisterTopologyTransactionResponseResult](
      requestedBy: Member,
      participant: ParticipantId,
      requestId: TopologyRequestId,
      results: Seq[Res],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): RegisterTopologyTransactionResponse[Res] =
    RegisterTopologyTransactionResponse(requestedBy, participant, requestId, results, domainId)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  private[messages] def fromProtoV0(
      message: v0.RegisterTopologyTransactionResponse
  ): ParsingResult[RegisterTopologyTransactionResponse[V0]] =
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
      results <- message.results.traverse(V0.fromProtoV0)
    } yield RegisterTopologyTransactionResponse(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      results,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))

  private[messages] def fromProtoV1(
      message: v1.RegisterTopologyTransactionResponse
  ): ParsingResult[RegisterTopologyTransactionResponse[V1]] =
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
      results <- message.results.traverse(V1.fromProtoV1)
    } yield RegisterTopologyTransactionResponse(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      results,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(1)))

  override def name: String = "RegisterTopologyTransactionResponse"

  /** Split the list of results between v0 and v1
    */
  private def splitResults(
      results: List[RegisterTopologyTransactionResponseResult]
  ): (List[V0], List[V1]) = {
    val (v0, v1) = results.foldLeft((List[V0](), List[V1]())) { case ((accV0, accV1), result) =>
      result match {
        case v0: V0 => (v0 +: accV0, accV1)
        case v1: V1 => (accV0, v1 +: accV1)
      }
    }

    (v0.reverse, v1.reverse)
  }

  def create(
      request: RegisterTopologyTransactionRequest,
      results: List[RegisterTopologyTransactionResponseResult],
      protocolVersion: ProtocolVersion,
  ): Either[Error, RegisterTopologyTransactionResponse[
    RegisterTopologyTransactionResponseResult
  ]] = {
    splitResults(results) match {
      case (Nil, v1) =>
        val response = RegisterTopologyTransactionResponse(
          request.requestedBy,
          request.participant,
          request.requestId,
          v1,
          request.domainId,
        )(RegisterTopologyTransactionResponse.protocolVersionRepresentativeFor(protocolVersion))

        Right(response)

      case (v0, Nil) =>
        val response = RegisterTopologyTransactionResponse(
          request.requestedBy,
          request.participant,
          request.requestId,
          v0,
          request.domainId,
        )(RegisterTopologyTransactionResponse.protocolVersionRepresentativeFor(protocolVersion))

        Right(response)

      case _ => Left(ResultVersionsMixture)
    }
  }

  implicit val registerTopologyTransactionResponseCast: ProtocolMessageContentCast[
    RegisterTopologyTransactionResponse[RegisterTopologyTransactionResponseResult]
  ] =
    ProtocolMessageContentCast
      .create[RegisterTopologyTransactionResponse[RegisterTopologyTransactionResponseResult]](
        "RegisterTopologyTransactionResponse"
      ) {
        case rttr: RegisterTopologyTransactionResponse[_] => Some(rttr)
        case _ => None
      }
}

sealed trait RegisterTopologyTransactionResponseResult {
  def state: RegisterTopologyTransactionResponseResult.State

  def toProtoV0: v0.RegisterTopologyTransactionResponse.Result
  def toProtoV1: v1.RegisterTopologyTransactionResponse.Result

  def isFailed: Boolean = state == RegisterTopologyTransactionResponseResult.State.Failed
}

object RegisterTopologyTransactionResponseResult {
  sealed trait State extends Product with Serializable with PrettyPrinting {
    override def pretty: Pretty[this.type] = prettyOfObject[this.type]
  }
  object State {
    @Deprecated
    case object Requested extends State

    case object Failed extends State

    case object Rejected extends State

    case object Accepted extends State

    case object Duplicate extends State

    /** Unnecessary removes are marked as obsolete */
    case object Obsolete extends State
  }

  private[messages] sealed abstract case class V0(uniquePathProtoPrimitive: String, state: State)
      extends RegisterTopologyTransactionResponseResult
      with PrettyPrinting {
    def toProtoV0: v0.RegisterTopologyTransactionResponse.Result = {
      def reply(state: v0.RegisterTopologyTransactionResponse.Result.State) =
        v0.RegisterTopologyTransactionResponse.Result(
          uniquePath = uniquePathProtoPrimitive,
          state = state,
          errorMessage = "",
        )

      state match {
        case State.Requested => reply(ProtoStateV0.REQUESTED)
        case State.Failed => reply(ProtoStateV0.FAILED)
        case State.Rejected => reply(ProtoStateV0.REJECTED)
        case State.Accepted => reply(ProtoStateV0.ACCEPTED)
        case State.Duplicate => reply(ProtoStateV0.DUPLICATE)
        case State.Obsolete => reply(ProtoStateV0.OBSOLETE)
      }
    }

    def toProtoV1: v1.RegisterTopologyTransactionResponse.Result =
      throw new UnsupportedOperationException("Cannot serialize a Result.V0 to Protobuf V1")

    override def pretty: Pretty[V0] = prettyOfClass(
      param("unique path proto primitive", _.uniquePathProtoPrimitive.unquoted),
      param("state", _.state),
    )
  }

  private[messages] object V0 {
    def apply(
        uniquePathProtoPrimitive: String,
        state: State,
    ): V0 = new V0(uniquePathProtoPrimitive, state) {}

    def fromProtoV0(result: v0.RegisterTopologyTransactionResponse.Result): ParsingResult[V0] =
      result.state match {
        case ProtoStateV0.MISSING_STATE =>
          Left(
            ProtoDeserializationError.OtherError(
              "Missing state for v0.RegisterTopologyTransactionResponse.State.Result"
            )
          )
        case ProtoStateV0.REQUESTED => Right(V0(result.uniquePath, State.Requested))
        case ProtoStateV0.FAILED => Right(V0(result.uniquePath, State.Failed))
        case ProtoStateV0.REJECTED => Right(V0(result.uniquePath, State.Rejected))
        case ProtoStateV0.ACCEPTED => Right(V0(result.uniquePath, State.Accepted))
        case ProtoStateV0.DUPLICATE => Right(V0(result.uniquePath, State.Duplicate))
        case ProtoStateV0.OBSOLETE => Right(V0(result.uniquePath, State.Obsolete))
        case ProtoStateV0.Unrecognized(unrecognizedValue) =>
          Left(
            ProtoDeserializationError.OtherError(
              s"Unrecognised state for v0.RegisterTopologyTransactionResponse.State.Result: $unrecognizedValue"
            )
          )
      }
  }

  private[messages] sealed abstract case class V1(state: State)
      extends RegisterTopologyTransactionResponseResult
      with PrettyPrinting {

    def toProtoV0: v0.RegisterTopologyTransactionResponse.Result =
      throw new UnsupportedOperationException("Cannot serialize a Result.V1 to Protobuf V0")

    def toProtoV1: v1.RegisterTopologyTransactionResponse.Result = {
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
        case State.Requested =>
          throw new IllegalStateException("State.Requested not allowed in Result.V1")
      }
    }

    override def pretty: Pretty[V1] = prettyOfClass(
      param("state", _.state)
    )
  }

  private[messages] object V1 {
    def apply(state: State): V1 = new V1(state) {}

    def fromProtoV1(result: v1.RegisterTopologyTransactionResponse.Result): ParsingResult[V1] = {
      result.state match {
        case ProtoStateV1.MISSING_STATE =>
          Left(
            ProtoDeserializationError.OtherError(
              "Missing state for v1.RegisterTopologyTransactionResponse.State.Result"
            )
          )
        case ProtoStateV1.FAILED => Right(V1(State.Failed))
        case ProtoStateV1.REJECTED => Right(V1(State.Rejected))
        case ProtoStateV1.ACCEPTED => Right(V1(State.Accepted))
        case ProtoStateV1.DUPLICATE => Right(V1(State.Duplicate))
        case ProtoStateV1.OBSOLETE => Right(V1(State.Obsolete))
        case ProtoStateV1.Unrecognized(unrecognizedValue) =>
          Left(
            ProtoDeserializationError.OtherError(
              s"Unrecognised state for v1.RegisterTopologyTransactionResponse.State.Result: $unrecognizedValue"
            )
          )
      }
    }
  }

  def create(
      uniquePathProtoPrimitive: String,
      state: State,
      protocolVersion: ProtocolVersion,
  ): RegisterTopologyTransactionResponseResult =
    if (protocolVersion >= ProtocolVersion.v4)
      V1(state)
    else V0(uniquePathProtoPrimitive, state)
}
