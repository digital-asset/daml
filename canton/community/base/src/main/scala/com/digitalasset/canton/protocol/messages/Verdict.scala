// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.utils.DeserializedCantonError
import com.daml.error.{ContextualizedErrorLogger, ErrorCategory}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  NotImplementedYet,
  OtherError,
  ValueDeserializationError,
}
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

  private[messages] def toProtoV0: v0.Verdict

  private[messages] def toProtoV1: v1.Verdict

  private[messages] def toProtoV2: v2.Verdict

  private[messages] def toProtoV3: v3.Verdict
}

object Verdict
    extends HasProtocolVersionedCompanion[Verdict]
    with ProtocolVersionedCompanionDbHelpers[Verdict] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.Verdict)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.Verdict)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.Verdict)(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v6)(v3.Verdict)(
      supportedProtoVersion(_)(fromProtoV3),
      _.toProtoV3.toByteString,
    ),
  )

  final case class Approve()(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends Verdict {
    override def isTimeoutDeterminedByMediator: Boolean = false

    private[messages] override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.Approve(empty.Empty()))

    private[messages] override def toProtoV1: v1.Verdict =
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.Approve(empty.Empty()))

    private[messages] override def toProtoV2: v2.Verdict =
      v2.Verdict(someVerdict = v2.Verdict.SomeVerdict.Approve(empty.Empty()))

    private[messages] override def toProtoV3: v3.Verdict =
      v3.Verdict(someVerdict = v3.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def pretty: Pretty[Verdict] = prettyOfString(_ => "Approve")
  }

  object Approve {
    def apply(protocolVersion: ProtocolVersion): Approve = Approve()(
      Verdict.protocolVersionRepresentativeFor(protocolVersion)
    )
  }

  sealed trait MediatorReject extends Verdict with TransactionRejection

  /** Used only with protocol version [[MediatorRejectV0.applicableProtocolVersion]] */
  final case class MediatorRejectV0 private (code: v0.MediatorRejection.Code, reason: String)
      extends MediatorReject {
    import MediatorRejectV0.*

    locally {
      import v0.MediatorRejection.Code
      val validCode = code match {
        case Code.MissingCode | Code.Unrecognized(_) => false
        case _ => true
      }
      require(validCode, s"Invalid mediator rejection code $code")
    }

    override def representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type] =
      Verdict.protocolVersionRepresentativeFor(ProtocolVersion.v3)

    def toProtoMediatorRejectionV0: v0.MediatorRejection =
      v0.MediatorRejection(code = code, reason = reason)

    private[messages] override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectionV0))

    private[messages] override def toProtoV1: v1.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    private[messages] override def toProtoV2: v2.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    private[messages] override def toProtoV3: v3.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    override def pretty: Pretty[MediatorRejectV0] = prettyOfClass(
      param("code", _.code.toString.unquoted),
      param("reason", _.reason.unquoted),
    )

    private def asError: BaseCantonError = {
      import v0.MediatorRejection.Code
      code match {
        case Code.InformeesNotHostedOnActiveParticipant | Code.InvalidRootHashMessage =>
          MediatorError.InvalidMessage.Reject(reason, code)
        case Code.NotEnoughConfirmingParties | Code.ViewThresholdBelowMinimumThreshold |
            Code.WrongDeclaredMediator | Code.NonUniqueRequestUuid =>
          MediatorError.MalformedMessage.Reject(reason, code)
        case Code.Timeout =>
          MediatorError.Timeout.Reject(reason)
        case Code.MissingCode | Code.Unrecognized(_) =>
          throw new IllegalStateException(s"Code ${code} is forbidden by the class invariant")
      }
    }

    override def logWithContext(extra: Map[String, String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Unit = asError.logWithContext(extra)

    override def rpcStatusWithoutLoggingContext(): Status = asError.rpcStatusWithoutLoggingContext()

    override def isTimeoutDeterminedByMediator: Boolean = code == v0.MediatorRejection.Code.Timeout
  }
  object MediatorRejectV0 {
    def applicableProtocolVersion: ProtocolVersion = ProtocolVersion.v3

    private[messages] def wrongProtocolVersion(rpv: RepresentativeProtocolVersion[_]): String =
      s"MediatorRejectV0 can only be used in representative protocol version $applicableProtocolVersion; found: representative protocol version=$rpv"

    def tryCreate(code: v0.MediatorRejection.Code, reason: String): MediatorRejectV0 =
      new MediatorRejectV0(code, reason)

    def create(code: v0.MediatorRejection.Code, reason: String): Either[String, MediatorRejectV0] =
      Either
        .catchOnly[IllegalArgumentException](new MediatorRejectV0(code, reason))
        .leftMap(_.getMessage)

    def fromProtoV0(
        value: v0.MediatorRejection
    ): ParsingResult[MediatorRejectV0] = {
      import v0.MediatorRejection.Code

      val v0.MediatorRejection(codeP, reason) = value
      for {
        validCode <- codeP match {
          case Code.MissingCode => Left(FieldNotSet("MediatorReject.code"))
          case Code.Unrecognized(code) =>
            Left(
              ValueDeserializationError(
                "reject",
                s"Unknown mediator rejection error code ${code} with ${reason}",
              )
            )
          case _ => Right(codeP)
        }
      } yield new MediatorRejectV0(validCode, reason)
    }
  }

  /** Used only with protocol versions from [[MediatorRejectV1.firstApplicableProtocolVersion]] to
    * [[MediatorRejectV1.lastApplicableProtocolVersion]].
    */
  final case class MediatorRejectV1 private (cause: String, id: String, errorCategory: Int)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends MediatorReject {
    import MediatorRejectV1.*

    require(
      representativeProtocolVersion >= protocolVersionRepresentativeFor(
        firstApplicableProtocolVersion
      ) &&
        representativeProtocolVersion <= protocolVersionRepresentativeFor(
          lastApplicableProtocolVersion
        ),
      wrongProtocolVersion(representativeProtocolVersion),
    )

    private[messages] override def toProtoV0: v0.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    private[messages] override def toProtoV1: v1.Verdict =
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectV1))

    private[messages] override def toProtoV2: v2.Verdict =
      v2.Verdict(someVerdict = v2.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectV1))

    private[messages] override def toProtoV3: v3.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    def toProtoMediatorRejectV1: v1.MediatorReject =
      v1.MediatorReject(cause = cause, errorCode = id, errorCategory = errorCategory)

    override def pretty: Pretty[MediatorRejectV1] =
      prettyOfClass(
        param("id", _.id.unquoted),
        param("cause", _.cause.unquoted),
      )

    override def logWithContext(extra: Map[String, String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Unit = asError.logWithContext(extra)

    override def rpcStatusWithoutLoggingContext(): Status = asError.rpcStatusWithoutLoggingContext()

    private def asError: BaseCantonError = id match {
      case MediatorError.Timeout.id =>
        MediatorError.Timeout.Reject(cause)
      case MediatorError.InvalidMessage.id =>
        MediatorError.InvalidMessage.Reject(cause)
      case MediatorError.MalformedMessage.id =>
        MediatorError.MalformedMessage.Reject(cause)
      case id =>
        val category = ErrorCategory
          .fromInt(errorCategory)
          .getOrElse(ErrorCategory.SystemInternalAssumptionViolated)
        MediatorError.GenericError(cause, id, category)
    }

    override def isTimeoutDeterminedByMediator: Boolean = id == MediatorError.Timeout.id
  }
  object MediatorRejectV1 {
    def firstApplicableProtocolVersion: ProtocolVersion = ProtocolVersion.v4
    def lastApplicableProtocolVersion: ProtocolVersion = ProtocolVersion.v5

    private[messages] def wrongProtocolVersion(pv: RepresentativeProtocolVersion[_]): String =
      s"MediatorRejectV1 can only be used in protocol versions from $firstApplicableProtocolVersion to $lastApplicableProtocolVersion; found: representative protocol version=$pv"

    private[messages] def tryCreate(
        cause: String,
        id: String,
        errorCategory: Int,
        rpv: RepresentativeProtocolVersion[Verdict.type],
    ): MediatorRejectV1 = new MediatorRejectV1(cause, id, errorCategory)(rpv)

    def tryCreate(
        cause: String,
        id: String,
        errorCategory: Int,
        protocolVersion: ProtocolVersion,
    ): MediatorRejectV1 =
      tryCreate(cause, id, errorCategory, Verdict.protocolVersionRepresentativeFor(protocolVersion))

    def fromProtoV1(
        mediatorRejectP: v1.MediatorReject,
        pv: RepresentativeProtocolVersion[Verdict.type],
    ): ParsingResult[MediatorRejectV1] = {
      val v1.MediatorReject(cause, errorCodeP, errorCategoryP) = mediatorRejectP
      Right(MediatorRejectV1(cause, errorCodeP, errorCategoryP)(pv))
    }
  }

  /** Used only with protocol version [[MediatorRejectV2.firstApplicableProtocolVersion]] and higher */
  final case class MediatorRejectV2 private (status: com.google.rpc.status.Status)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends MediatorReject {
    import MediatorRejectV2.*

    require(
      representativeProtocolVersion >= Verdict.protocolVersionRepresentativeFor(
        firstApplicableProtocolVersion
      ),
      wrongProtocolVersion(representativeProtocolVersion),
    )

    require(status.code != com.google.rpc.Code.OK_VALUE, "Rejection must not use status code OK")

    private[messages] override def toProtoV0: v0.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    private[messages] override def toProtoV1: v1.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    private[messages] override def toProtoV2: v2.Verdict =
      throw new UnsupportedOperationException(wrongProtocolVersion(representativeProtocolVersion))

    private[messages] override def toProtoV3: v3.Verdict =
      v3.Verdict(v3.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectV2))

    def toProtoMediatorRejectV2: v2.MediatorReject = v2.MediatorReject(Some(status))

    override def pretty: Pretty[MediatorRejectV2.this.type] = prettyOfClass(
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

  object MediatorRejectV2 {
    def firstApplicableProtocolVersion: ProtocolVersion = ProtocolVersion.v6

    private[messages] def wrongProtocolVersion(pv: RepresentativeProtocolVersion[_]): String =
      s"MediatorRejectV2 can only be used in protocol versions $firstApplicableProtocolVersion and higher; found: representative protocol version=$pv"

    private[messages] def tryCreate(
        status: com.google.rpc.status.Status,
        rpv: RepresentativeProtocolVersion[Verdict.type],
    ): MediatorRejectV2 =
      MediatorRejectV2(status)(rpv)

    def tryCreate(
        status: com.google.rpc.status.Status,
        protocolVersion: ProtocolVersion,
    ): MediatorRejectV2 =
      tryCreate(status, Verdict.protocolVersionRepresentativeFor(protocolVersion))

    private[messages] def fromProtoV2(
        mediatorRejectP: v2.MediatorReject
    ): ParsingResult[MediatorRejectV2] = {
      // Proto version 3 because mediator rejections are versioned according to verdicts
      // and verdicts use mediator reject V2 in proto version 3.
      val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(3))

      val v2.MediatorReject(statusO) = mediatorRejectP
      for {
        status <- ProtoConverter.required("rejection_reason", statusO)
      } yield MediatorRejectV2(status)(representativeProtocolVersion)
    }
  }

  /** @param reasons Mapping from the parties of a [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    *                to the rejection reason from the [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    */
  final case class ParticipantReject(reasons: NonEmpty[List[(Set[LfPartyId], LocalReject)]])(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict.type]
  ) extends Verdict {

    private[messages] override def toProtoV0: v0.Verdict = {
      val reasonsP = v0.RejectionReasons(reasons.map { case (parties, message) =>
        v0.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV0))
      })
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.ValidatorReject(reasonsP))
    }

    private[messages] override def toProtoV1: v1.Verdict = {
      val reasonsP = v1.ParticipantReject(reasons.map { case (parties, message) =>
        v0.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV0))
      })
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.ParticipantReject(reasonsP))
    }

    private[messages] override def toProtoV2: v2.Verdict = {
      val reasonsP = v2.ParticipantReject(reasons.map { case (parties, message) =>
        v2.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV1))
      })
      v2.Verdict(someVerdict = v2.Verdict.SomeVerdict.ParticipantReject(reasonsP))
    }

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

    private[messages] def fromProtoV0(
        rejectionReasonsP: v0.RejectionReasons
    ): ParsingResult[ParticipantReject] = {
      val v0.RejectionReasons(reasonsP) = rejectionReasonsP
      fromProtoRejectionReasonsV0(reasonsP, protocolVersionRepresentativeFor(ProtoVersion(0)))
    }

    private def fromProtoRejectionReasonsV0(
        reasonsP: Seq[v0.RejectionReason],
        pv: RepresentativeProtocolVersion[Verdict.type],
    ): ParsingResult[ParticipantReject] =
      for {
        reasons <- reasonsP.traverse(fromProtoReasonV0)
        reasonsNE <- NonEmpty
          .from(reasons.toList)
          .toRight(InvariantViolation("Field reasons must not be empty!"))
      } yield ParticipantReject(reasonsNE)(pv)

    private[messages] def fromProtoV1(
        participantRejectP: v1.ParticipantReject,
        pv: RepresentativeProtocolVersion[Verdict.type],
    ): ParsingResult[ParticipantReject] = {
      val v1.ParticipantReject(reasonsP) = participantRejectP
      fromProtoRejectionReasonsV0(reasonsP, pv)
    }

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

  private[messages] def fromProtoV0(verdictP: v0.Verdict): ParsingResult[Verdict] = {
    val v0.Verdict(someVerdictP) = verdictP
    import v0.Verdict.{SomeVerdict as V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(0))

    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve()(representativeProtocolVersion))
      case V.Timeout(empty.Empty(_)) =>
        Right(
          MediatorRejectV0.tryCreate(
            v0.MediatorRejection.Code.Timeout,
            MediatorError.Timeout.Reject.defaultCause,
          )
        )
      case V.MediatorReject(mediatorRejectionP) => MediatorRejectV0.fromProtoV0(mediatorRejectionP)
      case V.ValidatorReject(rejectionReasonsP) => ParticipantReject.fromProtoV0(rejectionReasonsP)
      case V.Empty => Left(NotImplementedYet("empty verdict type"))
    }
  }

  private[messages] def fromProtoV1(verdictP: v1.Verdict): ParsingResult[Verdict] = {
    val v1.Verdict(someVerdictP) = verdictP
    import v1.Verdict.{SomeVerdict as V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(1))

    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve()(representativeProtocolVersion))
      case V.MediatorReject(mediatorRejectP) =>
        MediatorRejectV1.fromProtoV1(mediatorRejectP, representativeProtocolVersion)
      case V.ParticipantReject(participantRejectP) =>
        ParticipantReject.fromProtoV1(participantRejectP, representativeProtocolVersion)
      case V.Empty => Left(NotImplementedYet("empty verdict type"))
    }
  }

  def fromProtoV2(verdictP: v2.Verdict): ParsingResult[Verdict] = {
    val v2.Verdict(someVerdictP) = verdictP
    import v2.Verdict.{SomeVerdict as V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(2))

    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve()(representativeProtocolVersion))
      case V.MediatorReject(mediatorRejectP) =>
        MediatorRejectV1.fromProtoV1(mediatorRejectP, representativeProtocolVersion)
      case V.ParticipantReject(participantRejectP) =>
        ParticipantReject.fromProtoV2(participantRejectP, representativeProtocolVersion)
      case V.Empty => Left(OtherError("empty verdict type"))
    }
  }

  def fromProtoV3(verdictP: v3.Verdict): ParsingResult[Verdict] = {
    val v3.Verdict(someVerdictP) = verdictP
    import v3.Verdict.{SomeVerdict as V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(3))
    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve()(representativeProtocolVersion))
      case V.MediatorReject(mediatorRejectP) =>
        MediatorRejectV2.fromProtoV2(mediatorRejectP)
      case V.ParticipantReject(participantRejectP) =>
        ParticipantReject.fromProtoV2(participantRejectP, representativeProtocolVersion)
      case V.Empty => Left(OtherError("empty verdict type"))
    }
  }

  private def fromProtoReasonV0(
      protoReason: v0.RejectionReason
  ): ParsingResult[(Set[LfPartyId], LocalReject)] = {
    val v0.RejectionReason(partiesP, messageP) = protoReason
    for {
      parties <- partiesP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      message <- ProtoConverter.parseRequired(LocalReject.fromProtoV0, "reject", messageP)
    } yield (parties, message)
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
