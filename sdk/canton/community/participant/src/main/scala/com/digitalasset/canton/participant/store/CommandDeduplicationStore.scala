// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.OptionT
import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp}
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.submission.{
  ChangeIdHash,
  SerializableSubmissionId,
}
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.participant.store.db.DbCommandDeduplicationStore
import com.digitalasset.canton.participant.store.memory.InMemoryCommandDeduplicationStore
import com.digitalasset.canton.protocol.StoredParties
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.digitalasset.canton.{ApplicationId, CommandId, LedgerSubmissionId}
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

trait CommandDeduplicationStore extends AutoCloseable {

  /** Returns the [[CommandDeduplicationData]] associated with the given
    * [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]], if any.
    */
  def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, CommandDeduplicationData]

  /** Updates the [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]'s for the given
    * [[com.digitalasset.canton.ledger.participant.state.ChangeId]]s with the given [[DefiniteAnswerEvent]]s.
    * The [[scala.Boolean]] specifies whether the definite answer is an acceptance (or rejection) of the command.
    *
    * Does not overwrite the data if the existing data has a higher [[DefiniteAnswerEvent.offset]]. This should never
    * happen in practice.
    */
  def storeDefiniteAnswers(answers: Seq[(ChangeId, DefiniteAnswerEvent, Boolean)])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Updates the [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]'s for the given
    * [[com.digitalasset.canton.ledger.participant.state.ChangeId]] with the given [[DefiniteAnswerEvent]].
    *
    * Does not overwrite the data if the existing data has a higher [[DefiniteAnswerEvent.offset]]. This should never
    * happen in practice.
    */
  def storeDefiniteAnswer(
      changeId: ChangeId,
      definiteAnswerEvent: DefiniteAnswerEvent,
      accepted: Boolean,
  ): FutureUnlessShutdown[Unit] =
    storeDefiniteAnswers(Seq((changeId, definiteAnswerEvent, accepted)))(
      definiteAnswerEvent.traceContext
    )

  /** Prunes all command deduplication entries whose [[CommandDeduplicationData.latestDefiniteAnswer]] offset
    * is less or equal to `upToInclusive`.
    *
    * @param prunedPublicationTime The publication time of the given offset
    */
  def prune(upToInclusive: AbsoluteOffset, prunedPublicationTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Returns the highest offset with which [[prune]] was called, and an upper bound on its publication time, if any.
    */
  def latestPruning()(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, OffsetAndPublicationTime]
}

object CommandDeduplicationStore {

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      releaseProtocolVersion: ReleaseProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): CommandDeduplicationStore =
    storage match {
      case _: MemoryStorage => new InMemoryCommandDeduplicationStore(loggerFactory)
      case jdbc: DbStorage =>
        new DbCommandDeduplicationStore(jdbc, timeouts, releaseProtocolVersion, loggerFactory)
    }

  final case class OffsetAndPublicationTime(
      offset: AbsoluteOffset,
      publicationTime: CantonTimestamp,
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[OffsetAndPublicationTime] = prettyOfClass(
      param("offset", _.offset),
      param("publication time", _.publicationTime),
    )
  }

  object OffsetAndPublicationTime {
    implicit val getResultOffsetAndPublicationTime: GetResult[OffsetAndPublicationTime] =
      GetResult { r =>
        val offset = r.<<[AbsoluteOffset]
        val publicationTime = r.<<[CantonTimestamp]
        OffsetAndPublicationTime(offset, publicationTime)
      }
  }
}

/** The command deduplication data associated with a [[com.digitalasset.canton.ledger.participant.state.ChangeId]].
  *
  * @param changeId The change ID this command deduplication data is associated with
  * @param latestDefiniteAnswer The latest definite answer for the change ID
  * @param latestAcceptance The latest accepting completion for the change ID, if any
  */
final case class CommandDeduplicationData private (
    changeId: ChangeId,
    latestDefiniteAnswer: DefiniteAnswerEvent,
    latestAcceptance: Option[DefiniteAnswerEvent],
) extends PrettyPrinting {
  import com.digitalasset.canton.participant.pretty.Implicits.*

  latestAcceptance.foreach { acceptance =>
    if (acceptance.offset > latestDefiniteAnswer.offset) {
      throw CommandDeduplicationData.InvalidCommandDeduplicationData(
        s"The latest definite answer at offset ${latestDefiniteAnswer.offset} is before the acceptance at offset ${acceptance.offset}"
      )
    }
  }

  override protected def pretty: Pretty[CommandDeduplicationData] = prettyOfClass(
    param("change id", _.changeId),
    param("latest definite answer", _.latestDefiniteAnswer),
    paramIfDefined("latest acceptance", _.latestAcceptance),
  )
}

object CommandDeduplicationData {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  final case class InvalidCommandDeduplicationData(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  def create(
      changeId: ChangeId,
      latestDefiniteAnswer: DefiniteAnswerEvent,
      latestAcceptance: Option[DefiniteAnswerEvent],
  ): Either[String, CommandDeduplicationData] =
    Either
      .catchOnly[InvalidCommandDeduplicationData](
        tryCreate(changeId, latestDefiniteAnswer, latestAcceptance)
      )
      .leftMap(_.getMessage)

  def tryCreate(
      changeId: ChangeId,
      latestDefiniteAnswer: DefiniteAnswerEvent,
      latestAcceptance: Option[DefiniteAnswerEvent],
  ): CommandDeduplicationData =
    new CommandDeduplicationData(changeId, latestDefiniteAnswer, latestAcceptance)

  implicit def getResultCommandDeduplicationData(implicit
      getResultByteArray: GetResult[Array[Byte]],
      getResultByteArrayO: GetResult[Option[Array[Byte]]],
  ): GetResult[CommandDeduplicationData] = GetResult { r =>
    val applicationId = r.<<[ApplicationId]
    val commandId = r.<<[CommandId]
    val actAs = r.<<[StoredParties]
    val latestDefiniteAnswer = r.<<[DefiniteAnswerEvent]
    val latestAcceptance = r.<<[Option[DefiniteAnswerEvent]]
    val changeId = ChangeId(applicationId.unwrap, commandId.unwrap, actAs.parties)
    create(changeId, latestDefiniteAnswer, latestAcceptance).valueOr(err =>
      throw new DbDeserializationException(
        s"Failed to deserialize command deduplication data: $err"
      )
    )
  }
}

/** @param offset A completion offset
  * @param publicationTime The publication time associated with the `offset`
  * @param traceContext The trace context that created the completion offset.
  */
final case class DefiniteAnswerEvent(
    offset: AbsoluteOffset,
    publicationTime: CantonTimestamp,
    submissionIdO: Option[LedgerSubmissionId],
    // TODO(#7348) add submission rank
)(val traceContext: TraceContext)
    extends PrettyPrinting {

  def serializableSubmissionId: Option[SerializableSubmissionId] =
    submissionIdO.map(SerializableSubmissionId(_))

  override protected def pretty: Pretty[DefiniteAnswerEvent] = prettyOfClass(
    param("offset", _.offset),
    param("publication time", _.publicationTime),
    paramIfNonEmpty("submission id", _.submissionIdO),
    param("trace context", _.traceContext),
  )
}

object DefiniteAnswerEvent {
  implicit def getResultDefiniteAnswerEvent(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[DefiniteAnswerEvent] = GetResult { r =>
    val offset = r.<<[AbsoluteOffset]
    val publicationTime = r.<<[CantonTimestamp]
    val submissionIdO = r.<<[Option[SerializableSubmissionId]]
    val traceContext = r.<<[SerializableTraceContext]
    DefiniteAnswerEvent(
      offset,
      publicationTime,
      submissionIdO.map(_.submissionId),
    )(traceContext.unwrap)
  }

  implicit def getResultDefinitionAnswerEventOption(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[DefiniteAnswerEvent]] =
    GetResult { r =>
      val offsetO = r.<<[Option[AbsoluteOffset]]
      val publicationTimeO = r.<<[Option[CantonTimestamp]]
      val submissionIdO = r.<<[Option[SerializableSubmissionId]]
      val traceContextO = r.<<[Option[SerializableTraceContext]]
      (offsetO, publicationTimeO, submissionIdO, traceContextO) match {
        case (Some(offset), Some(publicationTime), submissionId, Some(traceContext)) =>
          DefiniteAnswerEvent(
            offset,
            publicationTime,
            submissionId.map(_.submissionId),
          )(traceContext.unwrap).some
        case (None, None, None, None) => None
        case _ =>
          throw new DbDeserializationException(
            s"Invalid definite answer event (should be all None or all defined): $offsetO, $publicationTimeO, $traceContextO"
          )
      }
    }
}
