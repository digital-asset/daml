// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.db.DbInFlightSubmissionStore
import com.digitalasset.canton.participant.store.memory.InMemoryInFlightSubmissionStore
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Backing store for [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]].
  *
  * An in-flight submission is uniquely identified by its
  * [[com.digitalasset.canton.ledger.participant.state.v2.ChangeId]].
  * [[com.digitalasset.canton.sequencing.protocol.MessageId]]s should be unique too,
  * but this is not enforced by the store.
  *
  * Every change to an individual submissions must execute atomically.
  * Bulk operations may interleave arbitrarily the atomic changes of the affected individual submissions
  * and therefore need not be atomic as a whole.
  */
trait InFlightSubmissionStore extends AutoCloseable {

  /** Retrieves the in-flight submission for the given
    * [[com.digitalasset.canton.ledger.participant.state.v2.ChangeId]] if one exists.
    */
  def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[Future, InFlightSubmission[SubmissionSequencingInfo]]

  /** Returns all unsequenced in-flight submissions on the given domain
    * whose [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]
    * is no later than `observedSequencingTime`.
    *
    * The in-flight submissions are not returned in any specific order.
    */
  def lookupUnsequencedUptoUnordered(domainId: DomainId, observedSequencingTime: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Seq[InFlightSubmission[UnsequencedSubmission]]]

  /** Returns all sequenced in-flight submissions on the given domain
    * whose [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission.sequencingTime]]
    * is no later than `sequencingTimeInclusive`.
    *
    * The in-flight submissions are not returned in any specific order.
    */
  def lookupSequencedUptoUnordered(domainId: DomainId, sequencingTimeInclusive: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Seq[InFlightSubmission[SequencedSubmission]]]

  /** Returns one of the in-flight submissions with the given [[com.digitalasset.canton.topology.DomainId]]
    * and [[com.digitalasset.canton.sequencing.protocol.MessageId]], if any.
    */
  def lookupSomeMessageId(domainId: DomainId, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Future[Option[InFlightSubmission[SubmissionSequencingInfo]]]

  /** Returns the earliest [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]
    * or [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission.sequencingTime]] in the store, if any,
    * for the given domain.
    */
  def lookupEarliest(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  /** Registers the given submission as being in flight and unsequenced
    * unless there already is an in-flight submission for the same change ID.
    *
    * This method MUST NOT run concurrently with a [[delete]] query for the same change ID and message ID.
    * When this method fails with an exception, it is unknown whether the submission was registered.
    *
    * @return A [[scala.Left$]] of the existing in-flight submission with the same change ID
    *         and a different [[com.digitalasset.canton.sequencing.protocol.MessageId]] if there is any.
    */
  def register(
      submission: InFlightSubmission[UnsequencedSubmission]
  ): EitherT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo], Unit]

  /** Updates the registration of the given unsequenced submission with its root hash information.
    *
    * If the root hash for the given submission has already been set by a previous call to this method,
    * the new update will be ignored.
    * If the given submission is not found in the store, the update will be ignored. We don't report
    * an error because it can happen e.g. if the max sequencing time has already elapsed and the timely
    * rejection published.
    *
    * This is done as a separate operation from [[register]] because the root hash is currently
    * not known at registration time.
    */
  def updateRegistration(
      submission: InFlightSubmission[UnsequencedSubmission],
      rootHash: RootHash,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Moves the submissions to the given domain
    * with the given [[com.digitalasset.canton.sequencing.protocol.MessageId]]s
    * from [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]]
    * to [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission]].
    */
  def observeSequencing(domainId: DomainId, submissions: Map[MessageId, SequencedSubmission])(
      implicit traceContext: TraceContext
  ): Future[Unit]

  /** Moves the submission with the given [[com.digitalasset.canton.protocol.RootHash]]
    * from [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]]
    * to [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission]].
    *
    * If a [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission]] with the given
    * [[com.digitalasset.canton.protocol.RootHash]] already exists:
    *
    *   - if the given submission was sequenced '''earlier''' than the existing one, it replaces it;
    *   - otherwise, this call will be ignored.
    *
    * As this method is called from the asynchronous part of message processing, this behavior ensures that
    * the in-flight submission tracker always ends up tracking the '''earliest''' request for a given
    * [[com.digitalasset.canton.protocol.RootHash]], independently of the order in which these calls are made,
    * in accordance with the replay prevention mechanism.
    *
    * If the later request writes first, the [[InFlightSubmissionStore]] contains stale data for the submission
    * request until the earlier request updates the row. This is fine because the stale information will only be
    * read by the [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]] after the
    * corresponding completion event has been published by the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]].
    * However, this happens only after the earlier request has signalled its tick, i.e., when Phase 3 has finished (via the
    * [[com.digitalasset.canton.participant.protocol.Phase37Synchronizer]] and either a
    * [[com.digitalasset.canton.protocol.messages.MediatorResult]] has been processed or the decision time has elapsed.
    * By this time, the row with the stale data has been overwritten by the earlier request.
    *
    * Calls to this method also race with calls to [[observeSequencing]] for later messages, e.g., if a submission
    * request gets preplayed without a message ID. The argument about the stale data being benign also applies to
    * those races. There are no races between several calls to [[observeSequencing]] because [[observeSequencing]]
    * is called sequentially for each batch of sequenced events.
    */
  def observeSequencedRootHash(
      rootHash: RootHash,
      submission: SequencedSubmission,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Deletes the referred to in-flight submissions if there are any.
    *
    * If the [[com.digitalasset.canton.sequencing.protocol.MessageId]] in
    * [[com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightByMessageId]] is not a UUID,
    * there cannot be a matching in-flight submission because [[register]] forces a UUID for the message ID.
    */
  // We do not provide a `deleteUnsequencedUpto` as a counterpart to `lookupUnsequencedUpto`
  // so that we do not have to guard against concurrent insertions between the two calls,
  // e.g., if there comes a submission whose max sequencing times is derived from a very early ledger time.
  // This is also the reason for why we combine the change ID with the message ID.
  def delete(submissions: Seq[InFlightReference])(implicit traceContext: TraceContext): Future[Unit]

  /** Update the in-flight submission identified by the given `changeId`
    * if `submissionDomain` and `messageId` match and it is unsequenced and
    * the existing [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]
    * is not earlier than the `newSequencingInfo`'s
    * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]].
    * Only the field [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmission.sequencingInfo]]
    * is updated.
    *
    * This is useful to change when and how a rejection is reported, e.g.,
    * if the submission logic decided to not send the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
    * to the sequencer after all.
    */
  def updateUnsequenced(
      changeId: ChangeIdHash,
      submissionDomain: DomainId,
      messageId: MessageId,
      newSequencingInfo: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): Future[Unit]
}

object InFlightSubmissionStore {
  def apply(
      storage: Storage,
      maxItemsInSqlInClause: PositiveNumeric[Int],
      registerBatchAggregatorConfig: BatchAggregatorConfig,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): InFlightSubmissionStore = storage match {
    case _: MemoryStorage => new InMemoryInFlightSubmissionStore(loggerFactory)
    case jdbc: DbStorage =>
      new DbInFlightSubmissionStore(
        jdbc,
        maxItemsInSqlInClause,
        registerBatchAggregatorConfig,
        releaseProtocolVersion,
        timeouts,
        loggerFactory,
      )
  }

  /** Reference to an in-flight submission */
  sealed trait InFlightReference extends Product with Serializable with PrettyPrinting {
    def domainId: DomainId
    def toEither: Either[InFlightByMessageId, InFlightBySequencingInfo]
  }

  /** Identifies an in-flight submission via the [[com.digitalasset.canton.sequencing.protocol.MessageId]] */
  final case class InFlightByMessageId(override val domainId: DomainId, messageId: MessageId)
      extends InFlightReference {
    override def toEither: Either[InFlightByMessageId, InFlightBySequencingInfo] = Left(this)

    override def pretty: Pretty[InFlightByMessageId] = prettyOfClass(
      param("domain id", _.domainId),
      param("message id", _.messageId),
    )
  }

  /** Identifies an in-flight submission via the
    * [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission]]
    */
  final case class InFlightBySequencingInfo(
      override val domainId: DomainId,
      sequenced: SequencedSubmission,
  ) extends InFlightReference {
    override def toEither: Either[InFlightByMessageId, InFlightBySequencingInfo] = Right(this)

    override def pretty: Pretty[InFlightBySequencingInfo] = prettyOfClass(
      param("domain id", _.domainId),
      param("sequenced", _.sequenced),
    )
  }

}
