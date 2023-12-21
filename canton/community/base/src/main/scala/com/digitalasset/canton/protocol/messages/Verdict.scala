// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.error.utils.DeserializedCantonError
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, OtherError}
import com.digitalasset.canton.error.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.protobuf.empty
import com.google.rpc.status.Status
import pprint.Tree

import scala.Ordered.orderingToOrdered

trait TransactionRejection {
  def logWithContext(extra: Map[String, String] = Map())(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Unit

  def rpcStatusWithoutLoggingContext(): com.google.rpc.status.Status
}

/** Verdicts sent from the mediator to the participants inside the [[MediatorResult]] message */
sealed trait Verdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[Verdict] {

  /** Whether the verdict represents a timeout that the mediator has determined. */
  def isTimeoutDeterminedByMediator: Boolean

  @transient override protected lazy val companionObj: Verdict.type = Verdict

  private[messages] def toProtoV3: v3.Verdict
}

object Verdict
    extends HasProtocolVersionedCompanion[Verdict]
    with ProtocolVersionedCompanionDbHelpers[Verdict] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v30)(v3.Verdict)(
      supportedProtoVersion(_)(fromProtoV3),
      _.toProtoV3.toByteString,
    )
  )

  final case class Approve()(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends Verdict {
    override def isTimeoutDeterminedByMediator: Boolean = false

    private[messages] override def toProtoV3: v3.Verdict =
      v3.Verdict(someVerdict = v3.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def pretty: Pretty[Verdict] = prettyOfString(_ => "Approve")
  }

  object Approve {
    def apply(protocolVersion: ProtocolVersion): Approve = Approve()(
      Verdict.protocolVersionRepresentativeFor(protocolVersion)
    )
  }

  final case class MediatorReject private (status: com.google.rpc.status.Status)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends Verdict
      with TransactionRejection {
    require(status.code != com.google.rpc.Code.OK_VALUE, "Rejection must not use status code OK")

    private[messages] override def toProtoV3: v3.Verdict =
      v3.Verdict(v3.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectV2))

    def toProtoMediatorRejectV2: v2.MediatorReject = v2.MediatorReject(Some(status))

    override def pretty: Pretty[MediatorReject.this.type] = prettyOfClass(
      unnamedParam(_.status)
    )

    override def logWithContext(extra: Map[String, String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Unit =
      DeserializedCantonError.fromGrpcStatus(status) match {
        case Right(error) => error.logWithContext(extra)
        case Left(err) =>
          contextualizedErrorLogger.warn(s"Failed to parse mediator rejection: $err")
      }

    override def rpcStatusWithoutLoggingContext(): Status = status

    override def isTimeoutDeterminedByMediator: Boolean =
      DeserializedCantonError.fromGrpcStatus(status).exists(_.code.id == MediatorError.Timeout.id)
  }

  object MediatorReject {
    // TODO(#15628) Make it safe (intercept the exception and return an either)
    def tryCreate(
        status: com.google.rpc.status.Status,
        protocolVersion: ProtocolVersion,
    ): MediatorReject =
      MediatorReject(status)(Verdict.protocolVersionRepresentativeFor(protocolVersion))

    private[messages] def fromProtoV2(
        mediatorRejectP: v2.MediatorReject
    ): ParsingResult[MediatorReject] = {
      // Proto version 3 because mediator rejections are versioned according to verdicts
      // and verdicts use mediator reject V2 in proto version 3.
      val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(3))

      val v2.MediatorReject(statusO) = mediatorRejectP
      for {
        status <- ProtoConverter.required("rejection_reason", statusO)
      } yield MediatorReject(status)(representativeProtocolVersion)
    }
  }

  /** @param reasons Mapping from the parties of a [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    *                to the rejection reason from the [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    */
  final case class ParticipantReject(reasons: NonEmpty[List[(Set[LfPartyId], LocalReject)]])(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends Verdict {

    private[messages] override def toProtoV3: v3.Verdict = {
      val reasonsP = v2.ParticipantReject(reasons.map { case (parties, message) =>
        v2.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV1))
      })
      v3.Verdict(someVerdict = v3.Verdict.SomeVerdict.ParticipantReject(reasonsP))
    }

    override def pretty: Pretty[ParticipantReject] = {
      import Pretty.PrettyOps

      prettyOfClass(
        unnamedParam(
          _.reasons.map { case (parties, reason) =>
            Tree.Infix(reason.toTree, "- reported by:", parties.toTree)
          }
        )
      )
    }

    /** Returns the rejection reason with the highest [[com.daml.error.ErrorCategory]] */
    def keyEvent(implicit loggingContext: ErrorLoggingContext): LocalReject = {
      if (reasons.lengthCompare(1) > 0) {
        val message = show"Request was rejected with multiple reasons. $reasons"
        loggingContext.logger.info(message)(loggingContext.traceContext)
      }
      reasons.map { case (_, localReject) => localReject }.maxBy1(_.code.category)
    }

    override def isTimeoutDeterminedByMediator: Boolean = false
  }

  object ParticipantReject {
    def apply(
        reasons: NonEmpty[List[(Set[LfPartyId], LocalReject)]],
        protocolVersion: ProtocolVersion,
    ): ParticipantReject =
      ParticipantReject(reasons)(Verdict.protocolVersionRepresentativeFor(protocolVersion))

    private def fromProtoRejectionReasonsV2(
        reasonsP: Seq[v2.RejectionReason],
        pv: RepresentativeProtocolVersion[Verdict.type],
    ): ParsingResult[ParticipantReject] =
      for {
        reasons <- reasonsP.traverse(fromProtoReasonV2)
        reasonsNE <- NonEmpty
          .from(reasons.toList)
          .toRight(InvariantViolation("Field reasons must not be empty!"))
      } yield ParticipantReject(reasonsNE)(pv)

    def fromProtoV2(
        participantRejectP: v2.ParticipantReject,
        pv: RepresentativeProtocolVersion[Verdict.type],
    ): ParsingResult[ParticipantReject] = {
      val v2.ParticipantReject(reasonsP) = participantRejectP
      fromProtoRejectionReasonsV2(reasonsP, pv)
    }
  }

  override def name: String = "verdict"

  def fromProtoV3(verdictP: v3.Verdict): ParsingResult[Verdict] = {
    val v3.Verdict(someVerdictP) = verdictP
    import v3.Verdict.{SomeVerdict as V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(3))
    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve()(representativeProtocolVersion))
      case V.MediatorReject(mediatorRejectP) =>
        MediatorReject.fromProtoV2(mediatorRejectP)
      case V.ParticipantReject(participantRejectP) =>
        ParticipantReject.fromProtoV2(participantRejectP, representativeProtocolVersion)
      case V.Empty => Left(OtherError("empty verdict type"))
    }
  }

  private def fromProtoReasonV2(
      protoReason: v2.RejectionReason
  ): ParsingResult[(Set[LfPartyId], LocalReject)] = {
    val v2.RejectionReason(partiesP, messageP) = protoReason
    for {
      parties <- partiesP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      message <- ProtoConverter.parseRequired(LocalReject.fromProtoV1, "reject", messageP)
    } yield (parties, message)
  }
}
