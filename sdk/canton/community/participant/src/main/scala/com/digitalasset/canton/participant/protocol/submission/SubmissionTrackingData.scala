// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.Update.UnSequencedCommandRejected
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.{TransactionProcessor, v30}
import com.digitalasset.canton.participant.store.{
  SerializableCompletionInfo,
  SerializableRejectionReasonTemplate,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionedCompanionDbHelpers,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status

import java.util.UUID
import scala.annotation.unused

/** The data of an in-flight unsequenced submission that suffices to produce a rejection reason.
  * This data is persisted in the
  * [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] for unsequenced
  * submissions and updated when the corresponding a
  * [[com.digitalasset.canton.sequencing.protocol.DeliverError]] is processed or the submission
  * could not be sent to the sequencer.
  */
trait SubmissionTrackingData
    extends Product
    with Serializable
    with HasProtocolVersionedWrapper[SubmissionTrackingData]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: SubmissionTrackingData.type =
    SubmissionTrackingData

  protected def toProtoV30: v30.SubmissionTrackingData

  /** Produce a rejection event for the unsequenced submission using the given record time. */
  def rejectionEvent(recordTime: CantonTimestamp, messageUuid: UUID)(implicit
      loggingContext: NamedLoggingContext,
      traceContext: TraceContext,
  ): UnSequencedCommandRejected

  /** Update the tracking data so that the deliver error [[com.google.rpc.status.Status]] can be
    * taken into account by [[rejectionEvent]].
    *
    * @param timestamp
    *   The sequencer timestamp of the [[com.digitalasset.canton.sequencing.protocol.DeliverError]].
    * @param reason
    *   The reason for the deliver error generated by the sequencer.
    */
  def updateOnNotSequenced(timestamp: CantonTimestamp, reason: Status)(implicit
      loggingContext: NamedLoggingContext
  ): Option[UnsequencedSubmission]
}

object SubmissionTrackingData
    extends VersioningCompanion[SubmissionTrackingData]
    with ProtocolVersionedCompanionDbHelpers[SubmissionTrackingData] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v34), v30.SubmissionTrackingData)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      )
  )

  override def name: String = "submission tracking data"

  def fromProtoV30(
      submissionTrackingP: v30.SubmissionTrackingData
  ): ParsingResult[SubmissionTrackingData] = {
    val v30.SubmissionTrackingData(tracking) = submissionTrackingP
    tracking match {
      case v30.SubmissionTrackingData.Tracking.Transaction(transactionSubmissionTracking) =>
        TransactionSubmissionTrackingData.fromProtoV30(transactionSubmissionTracking)
      case v30.SubmissionTrackingData.Tracking.Empty => Left(FieldNotSet("tracking"))
    }
  }
}

/** Tracking data for transactions */
final case class TransactionSubmissionTrackingData(
    completionInfo: CompletionInfo,
    rejectionCause: TransactionSubmissionTrackingData.RejectionCause,
    synchronizerId: SynchronizerId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubmissionTrackingData.type
    ]
) extends SubmissionTrackingData
    with HasLoggerName {

  override def rejectionEvent(
      recordTime: CantonTimestamp,
      messageUuid: UUID,
  )(implicit
      loggingContext: NamedLoggingContext,
      traceContext: TraceContext,
  ): UnSequencedCommandRejected = {
    val reasonTemplate = rejectionCause.asFinalReason(recordTime)
    Update.UnSequencedCommandRejected(
      // notification will be tracked based on this as a non-sequenced in-flight reference
      completionInfo,
      reasonTemplate,
      synchronizerId,
      recordTime,
      messageUuid,
    )
  }

  override def updateOnNotSequenced(timestamp: CantonTimestamp, reason: Status)(implicit
      loggingContext: NamedLoggingContext
  ): Option[UnsequencedSubmission] =
    UnsequencedSubmission(
      timestamp,
      this.copy(rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(reason))(
        representativeProtocolVersion
      ),
    ).some

  protected def toProtoV30: v30.SubmissionTrackingData = {
    val completionInfoP = SerializableCompletionInfo(completionInfo).toProtoV30
    val transactionTracking = v30.TransactionSubmissionTrackingData(
      completionInfo = completionInfoP.some,
      rejectionCause = rejectionCause.toProtoV30.some,
      synchronizerId = synchronizerId.toProtoPrimitive,
    )
    v30.SubmissionTrackingData(v30.SubmissionTrackingData.Tracking.Transaction(transactionTracking))
  }

  override protected def pretty: Pretty[TransactionSubmissionTrackingData] = prettyOfClass(
    param("completion info", _.completionInfo),
    param("rejection cause", _.rejectionCause),
  )
}

object TransactionSubmissionTrackingData {
  def apply(
      completionInfo: CompletionInfo,
      rejectionCause: TransactionSubmissionTrackingData.RejectionCause,
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
  ): TransactionSubmissionTrackingData =
    TransactionSubmissionTrackingData(completionInfo, rejectionCause, synchronizerId)(
      SubmissionTrackingData.protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      tracking: v30.TransactionSubmissionTrackingData
  ): ParsingResult[TransactionSubmissionTrackingData] = {
    val v30.TransactionSubmissionTrackingData(completionInfoP, causeP, synchronizerIdP) = tracking
    for {
      completionInfo <- ProtoConverter.parseRequired(
        SerializableCompletionInfo.fromProtoV30,
        "completion info",
        completionInfoP,
      )
      cause <- ProtoConverter.parseRequired(RejectionCause.fromProtoV30, "rejection cause", causeP)
      synchronizerId <- SynchronizerId.fromProtoPrimitive(synchronizerIdP, "synchronizer_id")
      rpv <- SubmissionTrackingData.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TransactionSubmissionTrackingData(
      completionInfo,
      cause,
      synchronizerId,
    )(rpv)
  }

  trait RejectionCause extends Product with Serializable with PrettyPrinting {

    def asFinalReason(observedTimestamp: CantonTimestamp)(implicit
        loggingContext: ErrorLoggingContext
    ): Update.CommandRejected.FinalReason

    def toProtoV30: v30.TransactionSubmissionTrackingData.RejectionCause
  }

  object RejectionCause {
    def fromProtoV30(
        proto: v30.TransactionSubmissionTrackingData.RejectionCause
    ): ParsingResult[RejectionCause] = {
      val v30.TransactionSubmissionTrackingData.RejectionCause(cause) = proto
      cause match {
        case v30.TransactionSubmissionTrackingData.RejectionCause.Cause.Timeout(empty) =>
          TimeoutCause.fromProtoV30(empty)
        case v30.TransactionSubmissionTrackingData.RejectionCause.Cause
              .RejectionReasonTemplate(template) =>
          CauseWithTemplate.fromProtoV30(template)
        case v30.TransactionSubmissionTrackingData.RejectionCause.Cause.Empty =>
          Left(FieldNotSet("TransactionSubmissionTrackingData.RejectionCause.cause"))
      }
    }
  }

  case object TimeoutCause extends RejectionCause {

    override def asFinalReason(
        observedTimestamp: CantonTimestamp
    )(implicit loggingContext: ErrorLoggingContext): Update.CommandRejected.FinalReason = {
      val error = TransactionProcessor.SubmissionErrors.TimeoutError.Error(observedTimestamp)
      error.logWithContext()
      Update.CommandRejected.FinalReason(error.rpcStatus())
    }

    override def toProtoV30: v30.TransactionSubmissionTrackingData.RejectionCause =
      v30.TransactionSubmissionTrackingData.RejectionCause(
        cause = v30.TransactionSubmissionTrackingData.RejectionCause.Cause.Timeout(Empty())
      )

    override protected def pretty: Pretty[TimeoutCause.type] = prettyOfObject[TimeoutCause.type]

    def fromProtoV30(@unused _empty: Empty): ParsingResult[TimeoutCause.type] = Right(
      this
    )
  }

  final case class CauseWithTemplate(template: Update.CommandRejected.FinalReason)
      extends RejectionCause {
    override def asFinalReason(_observedTimestamp: CantonTimestamp)(implicit
        loggingContext: ErrorLoggingContext
    ): Update.CommandRejected.FinalReason = template

    override def toProtoV30: v30.TransactionSubmissionTrackingData.RejectionCause =
      v30.TransactionSubmissionTrackingData.RejectionCause(
        cause = v30.TransactionSubmissionTrackingData.RejectionCause.Cause.RejectionReasonTemplate(
          SerializableRejectionReasonTemplate(template.status).toProtoV30
        )
      )

    override protected def pretty: Pretty[CauseWithTemplate] = prettyOfClass(
      unnamedParam(_.template.status)
    )
  }

  object CauseWithTemplate {

    /** Log the `error` and then convert it into a
      * [[com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.FinalReason]]
      */
    def apply(
        error: TransactionError
    )(implicit loggingContext: ErrorLoggingContext): CauseWithTemplate = {
      error.logWithContext()
      CauseWithTemplate(Update.CommandRejected.FinalReason(error.rpcStatus()))
    }

    /** Log the `status` and then convert it into a
      * [[com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.FinalReason]]
      */
    def apply(
        status: Status
    )(implicit loggingContext: ErrorLoggingContext): CauseWithTemplate = {
      loggingContext.info(status.message)
      CauseWithTemplate(Update.CommandRejected.FinalReason(status))
    }

    def fromProtoV30(
        templateP: v30.CommandRejected.GrpcRejectionReasonTemplate
    ): ParsingResult[CauseWithTemplate] =
      for {
        templateStatus <- SerializableRejectionReasonTemplate.fromProtoV30(templateP)
      } yield CauseWithTemplate(Update.CommandRejected.FinalReason(templateStatus))
  }
}
