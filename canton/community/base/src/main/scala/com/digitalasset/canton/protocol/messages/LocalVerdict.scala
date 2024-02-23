// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.error.{ContextualizedErrorLogger, ErrorCategory, ErrorCode, NoLogging}
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.CreatesExistingContracts
import com.digitalasset.canton.protocol.LocalRejectError.{
  ConsistencyRejections,
  MalformedRejects,
  TimeRejects,
  TransferInRejects,
  TransferOutRejects,
}
import com.digitalasset.canton.protocol.messages.LocalVerdict.protocolVersionRepresentativeFor
import com.digitalasset.canton.protocol.{LocalRejectError, messages, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.protobuf.empty

/** Possible verdicts on a transaction view from the participant's perspective.
  * The verdict can be `LocalApprove`, `LocalReject` or `Malformed`.
  * The verdicts `LocalReject` and `Malformed` include a `reason` pointing out which checks in Phase 3 have failed.
  */
sealed trait LocalVerdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[LocalVerdict] {
  private[messages] def toProtoV30: v30.LocalVerdict

  @transient override protected lazy val companionObj: LocalVerdict.type = LocalVerdict

  override def representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
}

object LocalVerdict extends HasProtocolVersionedCompanion[LocalVerdict] {

  override def name: String = getClass.getSimpleName

  override def supportedProtoVersions: messages.LocalVerdict.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.LocalVerdict)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  private[messages] def fromProtoV30(
      localVerdictP: v30.LocalVerdict
  ): ParsingResult[LocalVerdict] = {
    import v30.LocalVerdict.SomeLocalVerdict as Lv

    val protocolVersion = protocolVersionRepresentativeFor(ProtoVersion(30))
    val v30.LocalVerdict(someLocalVerdictP) = localVerdictP

    someLocalVerdictP match {
      case Lv.LocalApprove(empty.Empty(_)) => protocolVersion.map(LocalApprove()(_))
      case Lv.LocalReject(localRejectP) => LocalReject.fromProtoV30(localRejectP)
      case Lv.Empty =>
        Left(OtherError("Unable to deserialize LocalVerdict, as the content is empty"))
    }
  }
}

final case class LocalApprove()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
) extends LocalVerdict {
  private[messages] def toProtoV30: v30.LocalVerdict =
    v30.LocalVerdict(v30.LocalVerdict.SomeLocalVerdict.LocalApprove(empty.Empty()))

  override def pretty: Pretty[this.type] = prettyOfClass()
}

object LocalApprove {
  def apply(protocolVersion: ProtocolVersion): LocalApprove =
    LocalApprove()(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
}

// TODO(i16856): temporary intermediate type, until the verdict LocalReject has been separated from the error LocalReject.
trait LocalReject extends LocalVerdict with TransactionRejection {

  def code: ErrorCode

  def rpcStatus(
      overrideCode: Option[io.grpc.Status.Code] = None
  )(implicit loggingContext: ContextualizedErrorLogger): com.google.rpc.status.Status

  override lazy val reason: com.google.rpc.status.Status = rpcStatus()(NoLogging)

  protected[messages] def toLocalRejectProtoV30: v30.LocalReject

  protected[messages] def toProtoV30: v30.LocalVerdict

}

object LocalReject {

  // list of local errors, used to map them during transport
  // if you add a new error below, you must add it to this list here as well
  private[messages] def fromProtoV30(
      localRejectP: v30.LocalReject
  ): ParsingResult[LocalReject] = {
    import ConsistencyRejections.*
    val v30.LocalReject(causePrefix, details, resource, errorCodeP, errorCategoryP) = localRejectP
    protocolVersionRepresentativeFor(ProtoVersion(30)).map { protocolVersion =>
      errorCodeP match {
        case LockedContracts.id => LockedContracts.Reject(resource)(protocolVersion)
        case InactiveContracts.id => InactiveContracts.Reject(resource)(protocolVersion)
        case CreatesExistingContracts.id =>
          CreatesExistingContracts.Reject(resource)(protocolVersion)
        case TimeRejects.LedgerTime.id =>
          TimeRejects.LedgerTime.Reject(details)(protocolVersion)
        case TimeRejects.SubmissionTime.id =>
          TimeRejects.SubmissionTime.Reject(details)(protocolVersion)
        case TimeRejects.LocalTimeout.id => TimeRejects.LocalTimeout.Reject()(protocolVersion)
        case MalformedRejects.MalformedRequest.id =>
          MalformedRejects.MalformedRequest.Reject(details)(protocolVersion)
        case MalformedRejects.Payloads.id =>
          MalformedRejects.Payloads.Reject(details)(protocolVersion)
        case MalformedRejects.ModelConformance.id =>
          MalformedRejects.ModelConformance.Reject(details)(protocolVersion)
        case MalformedRejects.BadRootHashMessages.id =>
          MalformedRejects.BadRootHashMessages.Reject(details)(protocolVersion)
        case TransferOutRejects.ActivenessCheckFailed.id =>
          TransferOutRejects.ActivenessCheckFailed.Reject(details)(protocolVersion)
        case TransferInRejects.AlreadyCompleted.id =>
          TransferInRejects.AlreadyCompleted.Reject(details)(protocolVersion)
        case TransferInRejects.ContractAlreadyActive.id =>
          TransferInRejects.ContractAlreadyActive.Reject(details)(protocolVersion)
        case TransferInRejects.ContractAlreadyArchived.id =>
          TransferInRejects.ContractAlreadyArchived.Reject(details)(protocolVersion)
        case TransferInRejects.ContractIsLocked.id =>
          TransferInRejects.ContractIsLocked.Reject(details)(protocolVersion)
        case id =>
          val category = ErrorCategory
            .fromInt(errorCategoryP)
            .getOrElse(ErrorCategory.SystemInternalAssumptionViolated)
          LocalRejectError.GenericReject(causePrefix, details, resource, id, category)(
            protocolVersion
          )
      }
    }
  }

}
