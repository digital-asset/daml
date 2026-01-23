// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, OtherError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.protocol.v30.LocalVerdict.VerdictCode.{
  VERDICT_CODE_LOCAL_ABSTAIN,
  VERDICT_CODE_LOCAL_APPROVE,
  VERDICT_CODE_LOCAL_MALFORMED,
  VERDICT_CODE_LOCAL_REJECT,
  VERDICT_CODE_UNSPECIFIED,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.rpc.status.Status

/** Possible verdicts on a transaction request from the participant's perspective. The verdict can
  * be `LocalApprove` or `LocalReject`. The verdict `LocalReject` includes a `reason` pointing out
  * which checks in Phase 3 have failed, and a flag `isMalformed` indicating whether the rejection
  * occurs due to malicious behavior.
  */
sealed trait LocalVerdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[LocalVerdict] {
  def kind: String

  def name: String = this match {
    case LocalApprove() => "an approval"
    case _: LocalReject => "a rejection"
    case _: LocalAbstain => "an abstention"
  }

  def isApprove: Boolean = this match {
    case _: LocalApprove => true
    case _ => false
  }

  def isReject: Boolean = this match {
    case _: LocalReject => true
    case _ => false
  }

  def isAbstain: Boolean = this match {
    case _: LocalAbstain => true
    case _ => false
  }

  def isMalformed: Boolean = false

  def reason: com.google.rpc.status.Status

  private[messages] def toProtoV30: v30.LocalVerdict

  @transient override protected lazy val companionObj: LocalVerdict.type = LocalVerdict

  override def representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
}

sealed trait NonPositiveLocalVerdict extends LocalVerdict {
  def logRejection(
      extra: Map[String, String]
  )(implicit errorLoggingContext: ErrorLoggingContext): Unit =
    // Log with level INFO, leave it to LocalRejectError to log the details.
    errorLoggingContext.withContext(extra) {
      lazy val action = if (isMalformed) "malformed" else "rejected"
      errorLoggingContext.info(show"Request is $action. $reason")
    }
}

object LocalVerdict extends VersioningCompanion[LocalVerdict] {

  override def name: String = getClass.getSimpleName

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.LocalVerdict)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[messages] def fromProtoV30(
      localVerdictP: v30.LocalVerdict
  ): ParsingResult[LocalVerdict] = {
    val v30.LocalVerdict(codeP, reasonPO) = localVerdictP
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      reason <- ProtoConverter.required("reason", reasonPO)
      localVerdict <- codeP match {
        case VERDICT_CODE_LOCAL_APPROVE => Right(LocalApprove()(rpv))
        case VERDICT_CODE_LOCAL_REJECT =>
          Right(LocalReject(reason, isMalformed = false)(rpv))
        case VERDICT_CODE_LOCAL_MALFORMED =>
          Right(LocalReject(reason, isMalformed = true)(rpv))
        case VERDICT_CODE_LOCAL_ABSTAIN => Right(LocalAbstain(reason)(rpv))
        case VERDICT_CODE_UNSPECIFIED => Left(FieldNotSet("LocalVerdict.code"))
        case v30.LocalVerdict.VerdictCode.Unrecognized(_) =>
          Left(
            OtherError(
              s"Unable to deserialize LocalVerdict due to invalid code $codeP."
            )
          )
      }
    } yield localVerdict
  }
}

final case class LocalApprove()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
) extends LocalVerdict {

  override def kind: String = "LocalApprove"
  override def reason: com.google.rpc.status.Status = Status(code = 0) // 0 means OK

  private[messages] def toProtoV30: v30.LocalVerdict =
    v30.LocalVerdict(
      code = VERDICT_CODE_LOCAL_APPROVE,
      reason = Some(reason),
    )

  override protected def pretty: Pretty[this.type] = prettyOfClass()
}

object LocalApprove {
  def apply(protocolVersion: ProtocolVersion): LocalApprove =
    LocalApprove()(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
}

final case class LocalReject(
    override val reason: com.google.rpc.status.Status,
    override val isMalformed: Boolean,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
) extends LocalVerdict
    with NonPositiveLocalVerdict
    with PrettyPrinting {

  override def kind: String = "LocalReject"

  override private[messages] def toProtoV30: v30.LocalVerdict = {
    val codeP =
      if (isMalformed) VERDICT_CODE_LOCAL_MALFORMED else VERDICT_CODE_LOCAL_REJECT
    v30.LocalVerdict(code = codeP, reason = Some(reason))
  }

  override protected def pretty: Pretty[LocalReject] = prettyOfClass(
    param("reason", _.reason),
    param("isMalformed", _.isMalformed),
  )
}

object LocalReject {
  def create(
      reason: com.google.rpc.status.Status,
      isMalformed: Boolean,
      protocolVersion: ProtocolVersion,
  ): LocalReject =
    LocalReject(reason, isMalformed)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
}

final case class LocalAbstain(override val reason: com.google.rpc.status.Status)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
) extends LocalVerdict
    with NonPositiveLocalVerdict {

  override def kind: String = "LocalAbstain"

  private[messages] def toProtoV30: v30.LocalVerdict =
    v30.LocalVerdict(
      code = VERDICT_CODE_LOCAL_ABSTAIN,
      reason = Some(reason),
    )

  override protected def pretty: Pretty[this.type] = prettyOfClass()
}
object LocalAbstain {
  def apply(reason: com.google.rpc.status.Status, protocolVersion: ProtocolVersion): LocalAbstain =
    LocalAbstain(reason)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
}
