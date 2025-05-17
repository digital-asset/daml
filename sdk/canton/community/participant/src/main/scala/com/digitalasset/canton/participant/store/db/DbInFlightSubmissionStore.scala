// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
  InFlightReference,
}
import com.digitalasset.canton.participant.store.db.DbInFlightSubmissionStore.RegisterProcessor.Result
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.db.DbBulkUpdateProcessor.BulkUpdatePendingCheck
import com.digitalasset.canton.store.db.{DbBulkUpdateProcessor, DbSerializationException}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.{PositionedParameters, SetParameter}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

class DbInFlightSubmissionStore(
    override protected val storage: DbStorage,
    registerBatchAggregatorConfig: BatchAggregatorConfig,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends InFlightSubmissionStore
    with DbStore { self =>

  import storage.api.*
  import storage.converters.*

  private implicit val setParameterSubmissionTrackingData: SetParameter[SubmissionTrackingData] =
    SubmissionTrackingData.getVersionedSetParameter

  override def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo]] =
    OptionT(storage.query(lookupQuery(changeIdHash), "lookup in-flight submission"))

  override def lookupUnsequencedUptoUnordered(
      synchronizerId: PhysicalSynchronizerId,
      observedSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[InFlightSubmission[UnsequencedSubmission]]] = {
    val query =
      sql"""
        select change_id_hash, submission_id, submission_physical_synchronizer_id, message_id, root_hash_hex, sequencing_timeout, tracking_data, trace_context
        from par_in_flight_submission where submission_physical_synchronizer_id = $synchronizerId and sequencing_timeout <= $observedSequencingTime
        """.as[InFlightSubmission[UnsequencedSubmission]]
    storage.query(query, "lookup unsequenced in-flight submission")
  }

  override def lookupSequencedUptoUnordered(
      synchronizerId: PhysicalSynchronizerId,
      sequencingTimeInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[InFlightSubmission[SequencedSubmission]]] = {
    val query =
      sql"""
        select change_id_hash, submission_id, submission_physical_synchronizer_id, message_id, root_hash_hex, sequencing_time, trace_context
        from par_in_flight_submission where submission_physical_synchronizer_id = $synchronizerId and sequencing_time <= $sequencingTimeInclusive
        """.as[InFlightSubmission[SequencedSubmission]]
    storage.query(query, "lookup sequenced in-flight submission")
  }

  override def lookupSomeMessageId(synchronizerId: PhysicalSynchronizerId, messageId: MessageId)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Option[InFlightSubmission[SubmissionSequencingInfo]]] = {
    val query =
      sql"""
        select change_id_hash, submission_id, submission_physical_synchronizer_id, message_id, root_hash_hex, sequencing_timeout, sequencing_time, tracking_data, trace_context
        from par_in_flight_submission where submission_physical_synchronizer_id = $synchronizerId and message_id = $messageId
        #${storage.limit(1)}
        """.as[InFlightSubmission[SubmissionSequencingInfo]].headOption
    storage.query(query, "lookup in-flight submission by message id")
  }

  override def lookupEarliest(
      synchronizerId: PhysicalSynchronizerId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val query =
      sql"""
        select min(sequencing_time), min(sequencing_timeout)
        from par_in_flight_submission where submission_physical_synchronizer_id = $synchronizerId
        """.as[(Option[CantonTimestamp], Option[CantonTimestamp])].headOption
    storage
      .query(query, "lookup earliest in-flight submission")
      .map(_.flatMap { case (earliestTimeout, earliestSequencing) =>
        OptionUtil.mergeWith(earliestTimeout, earliestSequencing)(Ordering[CantonTimestamp].min)
      })
  }

  override def register(
      submission: InFlightSubmission[UnsequencedSubmission]
  ): EitherT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo], Unit] = EitherT {
    implicit val traceContext: TraceContext = submission.submissionTraceContext

    def failOnNone[A](x: Option[A]): A = x.getOrElse {
      ErrorUtil.internalError(
        new IllegalStateException(s"Retry stopped early for submission $submission")
      )
    }

    batchAggregatorRegister.run(submission).flatMap(FutureUnlessShutdown.fromTry).map(failOnNone)

  }

  private val batchAggregatorRegister
      : BatchAggregator[InFlightSubmission[UnsequencedSubmission], Try[Result]] = {
    val processor =
      new DbInFlightSubmissionStore.RegisterProcessor(
        storage,
        releaseProtocolVersion,
        logger,
      )
    BatchAggregator(processor, registerBatchAggregatorConfig)
  }

  override def updateRegistration(
      submission: InFlightSubmission[UnsequencedSubmission],
      rootHash: RootHash,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateQuery =
      sqlu"""update par_in_flight_submission
             set root_hash_hex = $rootHash
             where submission_physical_synchronizer_id = ${submission.submissionSynchronizerId} and change_id_hash = ${submission.changeIdHash}
               and sequencing_timeout is not null and root_hash_hex is null
          """

    storage.update_(updateQuery, "update registration")
  }

  override def observeSequencing(
      synchronizerId: PhysicalSynchronizerId,
      submissions: Map[MessageId, SequencedSubmission],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateQuery =
      """update par_in_flight_submission
         set sequencing_timeout = null, tracking_data = null, sequencing_time = ?
         where submission_physical_synchronizer_id = ? and message_id = ? and sequencing_timeout is not null
      """
    val batchUpdate = DbStorage.bulkOperation_(updateQuery, submissions.toSeq, storage.profile) {
      pp => submission =>
        val (messageId, SequencedSubmission(sequencingTime)) = submission
        pp >> sequencingTime
        pp >> synchronizerId
        pp >> messageId
    }
    // No need for synchronous commit because this method is driven by the event stream from the sequencer,
    // which is the same across all replicas of the participant
    storage.queryAndUpdate(batchUpdate, "observe sequencing")
  }

  override def observeSequencedRootHash(
      rootHash: RootHash,
      submission: SequencedSubmission,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    sequencedRootHashBatchAggregator.run(SequencedRootHash(rootHash, submission))

  private val sequencedRootHashBatchAggregator = {
    val processor: BatchAggregator.Processor[SequencedRootHash, Unit] =
      new BatchAggregator.Processor[SequencedRootHash, Unit] {
        override def kind: String = "sequenced root hash"

        override def logger: TracedLogger = DbInFlightSubmissionStore.this.logger

        override def executeBatch(items: NonEmpty[Seq[Traced[SequencedRootHash]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[Iterable[Unit]] = {
          def setParams(pp: PositionedParameters)(data: Traced[SequencedRootHash]): Unit = {
            val Traced(SequencedRootHash(rootHash, submission)) = data
            val SequencedSubmission(ts) = submission

            pp >> ts
            pp >> rootHash
            pp >> ts
          }

          val action = DbStorage.bulkOperation_(
            """update par_in_flight_submission
               set sequencing_timeout = null, tracking_data = null, sequencing_time = ?
               where root_hash_hex = ? and (sequencing_timeout is not null or ? < sequencing_time)
            """,
            items,
            storage.profile,
          )(setParams)

          storage
            .queryAndUpdate(action, functionFullName)(traceContext, self.closeContext)
            .map(_ => Seq.fill(items.size)(()))
        }

        override def prettyItem: Pretty[SequencedRootHash] = implicitly
      }

    BatchAggregator(
      processor,
      registerBatchAggregatorConfig,
    )
  }

  case class SequencedRootHash(rootHash: RootHash, submission: SequencedSubmission)
      extends PrettyPrinting {
    override protected def pretty: Pretty[SequencedRootHash] =
      prettyOfClass(
        param("rootHash", _.rootHash),
        param("submission", _.submission),
      )
  }

  override def delete(
      submissions: Seq[InFlightReference]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val (byId, bySequencing) = submissions.toList.map(_.toEither).separate

    val byIdQuery =
      "delete from par_in_flight_submission where submission_physical_synchronizer_id = ? and message_id = ?"
    val batchById = DbStorage.bulkOperation_(byIdQuery, byId, storage.profile) { pp => submission =>
      val InFlightByMessageId(synchronizerId, messageId) = submission
      pp >> synchronizerId
      pp >> messageId
    }

    val bySequencingQuery =
      "delete from par_in_flight_submission where submission_physical_synchronizer_id = ? and sequencing_time = ?"
    val batchBySequencing =
      DbStorage.bulkOperation_(bySequencingQuery, bySequencing, storage.profile) {
        pp => submission =>
          val InFlightBySequencingInfo(synchronizerId, sequenced) = submission
          pp >> synchronizerId
          pp >> sequenced.sequencingTime
      }

    // No need for synchronous commits across DB replicas because this is driven off the Ledger API Indexer,
    // which ensures synchronization among `delete`s.
    // For the interaction with `register`, it is enough that `register` uses synchronous commits
    // as a synchronous commit ensures that all earlier commits in the WAL such as the delete
    // have also reached the DB replica.
    for {
      _ <- storage.queryAndUpdate(batchById, "delete submission by message id")
      _ <- storage.queryAndUpdate(batchBySequencing, "delete sequenced submission")
    } yield ()
  }

  override def updateUnsequenced(
      changeIdHash: ChangeIdHash,
      submissionSynchronizerId: PhysicalSynchronizerId,
      messageId: MessageId,
      newSequencingInfo: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateQuery =
      sqlu"""
          update par_in_flight_submission
          set sequencing_timeout = ${newSequencingInfo.timeout}, tracking_data = ${newSequencingInfo.trackingData}
          where change_id_hash = $changeIdHash and submission_physical_synchronizer_id = $submissionSynchronizerId and message_id = $messageId
            and sequencing_timeout >= ${newSequencingInfo.timeout}
          """
    // No need for synchronous commit here because this method is called only from the submission phase
    // after registration, so a fail-over participant would not call this method anyway.
    // The registered submission would simply time out in such a case.
    storage.update(updateQuery, functionFullName).flatMap {
      case 1 =>
        logger.debug(
          show"Updated unsequenced submission (change ID hash $changeIdHash, message ID $messageId) on $submissionSynchronizerId to $newSequencingInfo. "
        )
        FutureUnlessShutdown.unit
      case 0 =>
        // No update is reported. Let's see whether this is due to retries or races.
        lookup(changeIdHash).fold {
          // This case can happen if the max-sequencing time has already elapsed and the submission was already timely rejected.
          logger.info(
            s"Cannot update nonexistent submission (change ID hash $changeIdHash, message Id $messageId) on $submissionSynchronizerId.\nThis is OK if the submission has already timed out."
          )
        } { inFlight =>
          if (inFlight.sequencingInfo == newSequencingInfo) {
            // This can happen with underreporting or during crash recovery replay / reprocessing.
            logger.debug(
              show"Looked and found unsequenced submission (change ID hash $changeIdHash, message Id $messageId) on $submissionSynchronizerId with $newSequencingInfo"
            )
          } else
            inFlight.sequencingInfo.asUnsequenced match {
              case None =>
                logger.warn(
                  show"Submission (change ID hash $changeIdHash, message Id $messageId) on $submissionSynchronizerId has already been sequenced. ${inFlight.sequencingInfo}"
                )
              case Some(unsequenced) =>
                if (unsequenced.timeout < newSequencingInfo.timeout) {
                  logger.warn(
                    show"Sequencing timeout for submission (change ID hash $changeIdHash, message Id $messageId on $submissionSynchronizerId) is at ${unsequenced.timeout} before ${newSequencingInfo.timeout}. Current data: $unsequenced"
                  )
                } else {
                  // This should happen only if there are concurrent updates of unsequenced submissions.
                  // While the InFlightSubmissionTracker should be able to handle such a situation,
                  // it should not happen in practice.
                  logger.warn(
                    show"Failed to update unsequenced submission (change ID hash $changeIdHash, message Id $messageId) on $submissionSynchronizerId to $newSequencingInfo. Current data: ${inFlight.sequencingInfo}"
                  )
                }
            }
        }
      case rowCount =>
        ErrorUtil.internalErrorAsyncShutdown(
          new DbSerializationException(
            show"Failed to update unsequenced submission (change ID hash $changeIdHash, message Id $messageId) on $submissionSynchronizerId. Row count: $rowCount"
          )
        )
    }
  }

  private def lookupQuery(
      changeIdHash: ChangeIdHash
  ): DbAction.ReadTransactional[Option[InFlightSubmission[SubmissionSequencingInfo]]] =
    sql"""
        select change_id_hash, submission_id, submission_physical_synchronizer_id, message_id, root_hash_hex, sequencing_timeout, sequencing_time, tracking_data, trace_context
        from par_in_flight_submission where change_id_hash = $changeIdHash
        """.as[InFlightSubmission[SubmissionSequencingInfo]].headOption
}

object DbInFlightSubmissionStore {

  class RegisterProcessor(
      override protected val storage: DbStorage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      override val logger: TracedLogger,
  )(
      override protected implicit val executionContext: ExecutionContext,
      implicit val closeContext: CloseContext,
  ) extends DbBulkUpdateProcessor[InFlightSubmission[
        UnsequencedSubmission
      ], RegisterProcessor.Result] {
    import RegisterProcessor.Result
    import storage.api.*
    import storage.converters.*

    override def kind: String = "in-flight submission"

    private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
      SerializableTraceContext.getVersionedSetParameter(releaseProtocolVersion.v)
    private implicit val setParameterSubmissionTrackingData: SetParameter[SubmissionTrackingData] =
      SubmissionTrackingData.getVersionedSetParameter

    override def executeBatch(
        submissions: NonEmpty[Seq[Traced[InFlightSubmission[UnsequencedSubmission]]]]
    )(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[Iterable[Try[Result]]] = {

      type SubmissionAndCell =
        BulkUpdatePendingCheck[InFlightSubmission[UnsequencedSubmission], Result]

      // Allocate a cell for the result for each submission
      // The cell will be populated by one of the retries
      // or (if retrying stops prematurely due to an exception or shutdown) afterwards.
      val submissionsAndCells =
        submissions.toList.map(BulkUpdatePendingCheck(_, new SingleUseCell[Try[Result]]()))

      // Use a reference for the submissions that still need to be inserted,
      // so that we can pass information from one iteration to the next in the retry.
      val outstandingRef = new AtomicReference[List[SubmissionAndCell]](submissionsAndCells)

      def oneRound: FutureUnlessShutdown[Boolean] = {
        val outstanding = outstandingRef.get()
        bulkUpdateWithCheck(
          outstanding.map(_.target),
          "DbInFlightSubmissionStore.register",
        )(traceContext, closeContext).map { results =>
          val newOutstandingB = List.newBuilder[SubmissionAndCell]
          results.lazyZip(outstanding).foreach { (result, submissionAndCell) =>
            result match {
              case Success(None) =>
                // Retry on None
                newOutstandingB.addOne(submissionAndCell)
              case other =>
                submissionAndCell.cell.putIfAbsent(other).discard
            }
          }
          val newOutstanding = newOutstandingB.result()
          outstandingRef.set(newOutstanding)
          newOutstanding.isEmpty // Stop retrying if all submissions have been processed
        }
      }

      def unwrapCells: Seq[Try[Result]] = submissionsAndCells.map(_.cell.getOrElse {
        implicit val loggingContext = ErrorLoggingContext.fromTracedLogger(logger)
        val ex = new IllegalStateException("Bulk update did not provide a result")
        ErrorUtil.internalErrorTry(ex)
      })

      implicit val stopRetry: retry.Success[Boolean] = retry.Success[Boolean](Predef.identity)
      retry
        .Directly(logger, storage, retry.Forever, "register submission retry")
        .unlessShutdown(oneRound, NoExceptionRetryPolicy)
        .map(
          // Because we retry `Forever` until all cells are populated we can safely unwrap the cells
          _ => unwrapCells
        )
    }

    override protected def bulkUpdateAction(
        submissions: NonEmpty[Seq[Traced[InFlightSubmission[UnsequencedSubmission]]]]
    )(implicit
        batchTraceContext: TraceContext
    ): DBIOAction[Array[Int], NoStream, Effect.All] = {
      val insertQuery =
        """insert into par_in_flight_submission(
             change_id_hash, submission_id,
             submission_physical_synchronizer_id, message_id, root_hash_hex,
             sequencing_timeout, sequencing_time, tracking_data,
             trace_context)
           values (?, ?,
                   ?, ?, ?,
                   ?, NULL, ?,
                   ?)
           on conflict do nothing"""
      implicit val loggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      val bulkQuery = DbStorage.bulkOperation(
        insertQuery,
        submissions.map(_.value),
        storage.profile,
      ) { pp => submission =>
        import DbStorage.Implicits.*
        pp >> submission.changeIdHash
        pp >> submission.submissionId.map(SerializableSubmissionId(_))
        pp >> submission.submissionSynchronizerId
        pp >> submission.messageUuid
        pp >> submission.rootHashO
        pp >> submission.sequencingInfo.timeout
        pp >> submission.sequencingInfo.trackingData
        pp >> SerializableTraceContext(submission.submissionTraceContext)
      }
      // We need a synchronous commit here to ensure that there can be at most one submission
      // for the same change ID in-flight. Without synchronous commits,
      // a participant may have sent off a submission to the sequencer before this write reaches all DB replicas.
      // If a fail-over happens to another participant talking to the stale DB replica,
      // it may send off the same submission again to the sequencer.
      storage.withSyncCommitOnPostgres(bulkQuery)
    }

    private val success: Try[Result] = Success(Some(Either.unit))
    override protected def onSuccessItemUpdate(
        item: Traced[InFlightSubmission[UnsequencedSubmission]]
    ): Try[Result] = success

    override protected type CheckData = InFlightSubmission[SubmissionSequencingInfo]
    override protected type ItemIdentifier = ChangeIdHash
    override protected def itemIdentifier(
        submission: InFlightSubmission[UnsequencedSubmission]
    ): ChangeIdHash = submission.changeIdHash
    override protected def dataIdentifier(submission: CheckData): ChangeIdHash =
      submission.changeIdHash

    /** A list of queries for the items that we want to check for */
    override protected def checkQuery(submissionsToCheck: NonEmpty[Seq[ChangeIdHash]])(implicit
        batchTraceContext: TraceContext
    ): ReadOnly[immutable.Iterable[CheckData]] = {
      import DbStorage.Implicits.BuilderChain.*
      val query = sql"""
              select change_id_hash, submission_id, submission_physical_synchronizer_id, message_id, root_hash_hex, sequencing_timeout, sequencing_time, tracking_data, trace_context
              from par_in_flight_submission where """ ++ DbStorage.toInClause(
        "change_id_hash",
        submissionsToCheck,
      )
      query.as[InFlightSubmission[SubmissionSequencingInfo]]
    }
    override protected def analyzeFoundData(
        submission: InFlightSubmission[UnsequencedSubmission],
        foundData: Option[CheckData],
    )(implicit traceContext: TraceContext): Try[Result] = {
      // Retry if the conflicting submission has disappeared, i.e., `foundData == None`
      val response = foundData.map(existing => Either.cond(existing == submission, (), existing))
      Success(response)
    }

    override def prettyItem: Pretty[InFlightSubmission[UnsequencedSubmission]] = implicitly
  }

  object RegisterProcessor {
    // We retry inserting until we find a conflicting submission (Left) or have inserted the submission (Right).
    // The `Option` is `None` if we need to retry for the corresponding submission.
    type Result = Option[Either[InFlightSubmission[SubmissionSequencingInfo], Unit]]
  }
}
