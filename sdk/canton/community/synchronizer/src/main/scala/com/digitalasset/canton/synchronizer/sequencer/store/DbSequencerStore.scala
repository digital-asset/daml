// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import cats.Monad
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.catsinstances.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.*
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.*
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbParameterUtils, DbStorage}
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedEnvelope, MessageId}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.db.RequiredTypesCodec.*
import com.digitalasset.canton.synchronizer.block.UninitializedBlockHeight
import com.digitalasset.canton.synchronizer.sequencer.{
  CommitMode,
  SequencerMemberStatus,
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{BytesUnit, EitherTUtil, ErrorUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import org.h2.api.ErrorCode as H2ErrorCode
import org.postgresql.util.PSQLState
import slick.jdbc.*

import java.sql.SQLException
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.util.{Failure, Success}

/** Database backed sequencer store. Supports many concurrent instances reading and writing to the
  * same backing database.
  */
class DbSequencerStore(
    @VisibleForTesting private[canton] val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override val bufferedEventsMaxMemory: BytesUnit,
    bufferedEventsPreloadBatchSize: PositiveInt,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    override val sequencerMember: Member,
    override val blockSequencerMode: Boolean,
    cachingConfigs: CachingConfigs,
    override val batchingConfig: BatchingConfig,
    overrideCloseContext: Option[CloseContext] = None,
)(protected implicit val executionContext: ExecutionContext)
    extends SequencerStore
    with NamedLogging
    with FlagCloseable {

  import DbStorage.Implicits.*
  import Member.DbStorageImplicits.*
  import storage.api.*
  import storage.converters.*

  implicit val closeContext: CloseContext =
    overrideCloseContext
      .map(CloseContext.combineUnsafe(_, CloseContext(this), timeouts, logger)(TraceContext.empty))
      .getOrElse(CloseContext(this))

  private implicit val setRecipientsArrayOParameter
      : SetParameter[Option[NonEmpty[SortedSet[SequencerMemberId]]]] =
    (v, pp) => DbParameterUtils.setArrayIntOParameterDb(v.map(_.toArray.map(_.unwrap)), pp)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private implicit val getRecipientsArrayOResults
      : GetResult[Option[NonEmpty[SortedSet[SequencerMemberId]]]] =
    DbParameterUtils
      .getDataArrayOResultsDb[SequencerMemberId](
        storage.profile,
        deserialize = v => SequencerMemberId(v),
      )
      .andThen(_.map(arr => NonEmptyUtil.fromUnsafe(SortedSet(arr.toSeq*))))

  private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(protocolVersion)

  /** Single char that is persisted with the event to indicate the type of event */
  sealed abstract class EventTypeDiscriminator(val value: Char)

  object EventTypeDiscriminator {

    case object Receipt extends EventTypeDiscriminator('R')
    case object Deliver extends EventTypeDiscriminator('D')
    case object Error extends EventTypeDiscriminator('E')

    private val all = Seq[EventTypeDiscriminator](Deliver, Error, Receipt)

    def fromChar(value: Char): Either[String, EventTypeDiscriminator] =
      all.find(_.value == value).toRight(s"Event type discriminator for value [$value] not found")
  }

  private implicit val setEventTypeDiscriminatorParameter: SetParameter[EventTypeDiscriminator] =
    (etd, pp) => pp >> etd.value.toString

  private implicit val getEventTypeDiscriminatorResult: GetResult[EventTypeDiscriminator] =
    GetResult { r =>
      val value = r.nextString()

      val resultE = for {
        ch <- value.headOption.toRight("Event type discriminator is not set")
        etd <- EventTypeDiscriminator.fromChar(ch)
      } yield etd

      // there's nothing we can do from a `GetResult` with an error but throw
      resultE.fold(msg => throw new DbDeserializationException(msg), identity)
    }

  private implicit val getPayloadResult: GetResult[BytesPayload] =
    GetResult
      .createGetTuple2[PayloadId, ByteString]
      .andThen { case (id, content) =>
        BytesPayload(id, content)
      }

  /** @param trafficReceiptO
    *   If traffic management is enabled, there should always be traffic information for the sender.
    *   The information might be discarded later though, in case the event is being processed as
    *   part of a subscription for any of the recipients that isn't the sender.
    */
  case class DeliverStoreEventRow[P](
      timestamp: CantonTimestamp,
      instanceIndex: Int,
      eventType: EventTypeDiscriminator,
      messageIdO: Option[MessageId] = None,
      senderO: Option[SequencerMemberId] = None,
      recipientsO: Option[NonEmpty[SortedSet[SequencerMemberId]]] = None,
      payloadO: Option[P] = None,
      topologyTimestampO: Option[CantonTimestamp] = None,
      traceContext: TraceContext,
      // TODO(#15628) We should represent this differently, so that DeliverErrorStoreEvent.fromByteString parameter is always defined as well
      errorO: Option[ByteString],
      trafficReceiptO: Option[TrafficReceipt],
  ) {
    lazy val asStoreEvent: Either[String, Sequenced[P]] =
      for {
        event <- eventType match {
          case EventTypeDiscriminator.Deliver => asDeliverStoreEvent: Either[String, StoreEvent[P]]
          case EventTypeDiscriminator.Error => asErrorStoreEvent: Either[String, StoreEvent[P]]
          case EventTypeDiscriminator.Receipt => asReceiptStoreEvent: Either[String, StoreEvent[P]]
        }
      } yield Sequenced(timestamp, event)

    private lazy val asDeliverStoreEvent: Either[String, DeliverStoreEvent[P]] =
      for {
        messageId <- messageIdO.toRight("message-id not set for deliver event")
        sender <- senderO.toRight("sender not set for deliver event")
        recipients <- recipientsO.toRight("recipients not set for deliver event")
        payload <- payloadO.toRight("payload not set for deliver event")
      } yield DeliverStoreEvent(
        sender,
        messageId,
        recipients,
        payload,
        topologyTimestampO,
        traceContext,
        trafficReceiptO,
      )

    private lazy val asErrorStoreEvent: Either[String, DeliverErrorStoreEvent] =
      for {
        messageId <- messageIdO.toRight("message-id not set for deliver error")
        sender <- senderO.toRight("sender not set for deliver error")
      } yield DeliverErrorStoreEvent(
        sender,
        messageId,
        errorO,
        traceContext,
        trafficReceiptO,
      )

    private lazy val asReceiptStoreEvent: Either[String, ReceiptStoreEvent] =
      for {
        messageId <- messageIdO.toRight("message-id not set for receipt event")
        sender <- senderO.toRight("sender not set for receipt event")
      } yield ReceiptStoreEvent(
        sender,
        messageId,
        topologyTimestampO,
        traceContext,
        trafficReceiptO,
      )

  }

  object DeliverStoreEventRow {
    def apply(
        instanceIndex: Int,
        storeEvent: Sequenced[PayloadId],
    ): DeliverStoreEventRow[PayloadId] =
      storeEvent.event match {
        case DeliverStoreEvent(
              sender,
              messageId,
              members,
              payloadId,
              topologyTimestampO,
              traceContext,
              trafficReceiptO,
            ) =>
          DeliverStoreEventRow(
            storeEvent.timestamp,
            instanceIndex,
            EventTypeDiscriminator.Deliver,
            messageIdO = Some(messageId),
            senderO = Some(sender),
            recipientsO = Some(members),
            payloadO = Some(payloadId),
            topologyTimestampO = topologyTimestampO,
            traceContext = traceContext,
            errorO = None,
            trafficReceiptO = trafficReceiptO,
          )
        case DeliverErrorStoreEvent(sender, messageId, errorO, traceContext, trafficReceiptO) =>
          DeliverStoreEventRow(
            timestamp = storeEvent.timestamp,
            instanceIndex = instanceIndex,
            eventType = EventTypeDiscriminator.Error,
            messageIdO = Some(messageId),
            senderO = Some(sender),
            recipientsO =
              Some(NonEmpty(SortedSet, sender)), // must be set for sender to receive value
            traceContext = traceContext,
            errorO = errorO,
            trafficReceiptO = trafficReceiptO,
          )
        case ReceiptStoreEvent(
              sender,
              messageId,
              topologyTimestampO,
              traceContext,
              trafficReceiptO,
            ) =>
          DeliverStoreEventRow(
            timestamp = storeEvent.timestamp,
            instanceIndex = instanceIndex,
            eventType = EventTypeDiscriminator.Receipt,
            messageIdO = Some(messageId),
            senderO = Some(sender),
            recipientsO =
              Some(NonEmpty(SortedSet, sender)), // must be set for sender to receive value
            traceContext = traceContext,
            errorO = None,
            topologyTimestampO = topologyTimestampO,
            trafficReceiptO = trafficReceiptO,
          )
      }
  }

  private implicit val getPayloadOResult: GetResult[Option[BytesPayload]] =
    GetResult
      .createGetTuple2[Option[PayloadId], Option[ByteString]]
      .andThen {
        case (Some(id), Some(content)) => Some(BytesPayload(id, content))
        case (None, None) => None
        case (Some(id), None) =>
          throw new DbDeserializationException(s"Event row has payload id set [$id] but no content")
        case (None, Some(_)) =>
          throw new DbDeserializationException(
            "Event row has no payload id but has payload content"
          )
      }

  private implicit val trafficReceiptOGetResult: GetResult[Option[TrafficReceipt]] =
    GetResult
      .createGetTuple3[Option[NonNegativeLong], Option[NonNegativeLong], Option[NonNegativeLong]]
      .andThen {
        case (Some(consumedCost), Some(trafficConsumed), Some(baseTraffic)) =>
          Some(TrafficReceipt(consumedCost, trafficConsumed, baseTraffic))
        case (None, None, None) => None
        case (consumedCost, extraTrafficConsumed, baseTrafficRemainder) =>
          throw new DbDeserializationException(
            s"Inconsistent traffic data: consumedCost=$consumedCost, extraTrafficConsumed=$extraTrafficConsumed, baseTrafficRemained=$baseTrafficRemainder"
          )
      }

  private implicit val getDeliverStoreEventRowResultWithPayload
      : GetResult[Sequenced[BytesPayload]] = {
    val timestampGetter = implicitly[GetResult[CantonTimestamp]]
    val timestampOGetter = implicitly[GetResult[Option[CantonTimestamp]]]
    val discriminatorGetter = implicitly[GetResult[EventTypeDiscriminator]]
    val messageIdGetter = implicitly[GetResult[Option[MessageId]]]
    val memberIdGetter = implicitly[GetResult[Option[SequencerMemberId]]]
    val memberIdNesGetter = implicitly[GetResult[Option[NonEmpty[SortedSet[SequencerMemberId]]]]]
    val payloadGetter = implicitly[GetResult[Option[BytesPayload]]]
    val traceContextGetter = implicitly[GetResult[SerializableTraceContext]]
    val errorOGetter = implicitly[GetResult[Option[ByteString]]]
    val trafficReceipt = implicitly[GetResult[Option[TrafficReceipt]]]

    GetResult { r =>
      val row = DeliverStoreEventRow[BytesPayload](
        timestampGetter(r),
        r.nextInt(),
        discriminatorGetter(r),
        messageIdGetter(r),
        memberIdGetter(r),
        memberIdNesGetter(r),
        payloadGetter(r),
        timestampOGetter(r),
        traceContextGetter(r).unwrap,
        errorOGetter(r),
        trafficReceipt(r),
      )

      row.asStoreEvent.valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize event row: $err")
      )
    }
  }

  private implicit val getDeliverStoreEventRowResult: GetResult[Sequenced[PayloadId]] = {
    val timestampGetter = implicitly[GetResult[CantonTimestamp]]
    val timestampOGetter = implicitly[GetResult[Option[CantonTimestamp]]]
    val discriminatorGetter = implicitly[GetResult[EventTypeDiscriminator]]
    val messageIdGetter = implicitly[GetResult[Option[MessageId]]]
    val memberIdGetter = implicitly[GetResult[Option[SequencerMemberId]]]
    val memberIdNesGetter = implicitly[GetResult[Option[NonEmpty[SortedSet[SequencerMemberId]]]]]
    val payloadIdGetter = implicitly[GetResult[Option[PayloadId]]]
    val traceContextGetter = implicitly[GetResult[SerializableTraceContext]]
    val errorOGetter = implicitly[GetResult[Option[ByteString]]]
    val trafficReceipt = implicitly[GetResult[Option[TrafficReceipt]]]

    GetResult { r =>
      val row = DeliverStoreEventRow[PayloadId](
        timestampGetter(r),
        r.nextInt(),
        discriminatorGetter(r),
        messageIdGetter(r),
        memberIdGetter(r),
        memberIdNesGetter(r),
        payloadIdGetter(r),
        timestampOGetter(r),
        traceContextGetter(r).unwrap,
        errorOGetter(r),
        trafficReceipt(r),
      )

      row.asStoreEvent
        .fold(
          msg => throw new DbDeserializationException(s"Failed to deserialize event row: $msg"),
          identity,
        )
    }
  }

  private val profile = storage.profile

  private val (memberContainsBefore, memberContainsAfter): (String, String) = profile match {
    case _: Postgres =>
      ("", " = any(events.recipients)")
    case _: H2 =>
      ("array_contains(events.recipients, ", ")")
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private val payloadCache: TracedAsyncLoadingCache[FutureUnlessShutdown, PayloadId, BytesPayload] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, PayloadId, BytesPayload](
      cache = cachingConfigs.sequencerPayloadCache
        .buildScaffeine()
        .weigher((_: Any, v: Any) => v.asInstanceOf[BytesPayload].content.size),
      loader = implicit traceContext =>
        payloadId => readPayloadsFromStore(Seq(payloadId)).map(_(payloadId)),
      allLoader =
        Some(implicit traceContext => payloadIds => readPayloadsFromStore(payloadIds.toSeq)),
    )(logger, "payloadCache")

  override def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerMemberId] =
    storage.queryAndUpdate(
      for {
        _ <- profile match {
          case _: H2 =>
            sqlu"""merge into sequencer_members using dual
                    on member = $member
                    when not matched then
                      insert (member, registered_ts) values ($member, $timestamp)
                  """
          case _: Postgres =>
            sqlu"""insert into sequencer_members (member, registered_ts)
                  values ($member, $timestamp)
                  on conflict (member) do nothing
             """
        }
        id <- sql"select id from sequencer_members where member = $member"
          .as[SequencerMemberId]
          .head
      } yield id,
      "registerMember",
    )

  protected override def lookupMemberInternal(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RegisteredMember]] =
    storage.query(
      sql"""select id, registered_ts, enabled from sequencer_members where member = $member"""
        .as[(SequencerMemberId, CantonTimestamp, Boolean)]
        .headOption
        .map(_.map((RegisteredMember.apply _).tupled)),
      functionFullName,
    )

  /** In unified sequencer payload ids are deterministic (these are sequencing times from the block
    * sequencer), so we can somewhat safely ignore conflicts arising from sequencer restarts, crash
    * recovery, ha lock loses, unlike the complicate `savePayloads` method below
    */
  private def savePayloadsWithDiscriminator(
      payloads: NonEmpty[Seq[BytesPayload]],
      instanceDiscriminator: UUID,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] = {
    val insertSql =
      """insert into sequencer_payloads (id, instance_discriminator, content) values (?, ?, ?) on conflict do nothing"""

    EitherT.right[SavePayloadsError](
      storage
        .queryAndUpdate(
          DbStorage.bulkOperation(insertSql, payloads, storage.profile) { pp => payload =>
            pp >> payload.id.unwrap
            pp >> instanceDiscriminator
            pp >> payload.content
          },
          functionFullName,
        )
        .map(_ => ())
    )
  }

  /** Save the provided payloads to the store.
    *
    * For DB implementations we suspect that this will be a hot spot for performance primarily due
    * to size of the payload content values being inserted. To help with this we use
    * `storage.queryAndUpdateUnsafe` to do these inserts using the full connection pool of the node
    * instead of the single connection that is protected with an exclusive lock in the HA sequencer
    * setup.
    *
    * The downside of this optimization is that if a HA writer was to lose its lock, writes for
    * payloads will continue regardless for a period until it shuts down. During this time another
    * writer with the same instance index could come online. As we generate the payload id using a
    * value generated for a partition based on the instance index this could worse case mean that
    * two sequencer writers attempt to insert different payloads with the same id. If we use a
    * simple idempotency method of just ignoring conflicts this could result in the active sequencer
    * writing an event with a different payload resulting in a horrid corruption problem. This will
    * admittedly be difficult to hit, but possible all the same.
    *
    * The approach we use here is to generate an ephemeral instance discriminator for each writer
    * instance.
    *   - We insert the payloads using a simple insert (intentionally not ignoring conflicts unlike
    *     our other idempotent inserts). If this was successful we end here.
    *   - If this insert hits a payload with the same id the query will blow up with a unique
    *     constraint violation exception. Now there's a couple of reasons this may have happened:
    *     1. Another instance is running and has inserted a conflicting payload (bad!) 2. There was
    *        some connection issue when running this query and our storage layer didn't see a
    *        successful result so it retried our query unaware that it did actually succeed so now
    *        we conflicting with our own insert. (a bit of a shame but not really a problem).
    *   - We now query filtered on the payload ids we're attempting to insert to select out which
    *     payloads exist and their respective discriminators. For any payloads that exist we check
    *     that the discriminators indicate that we inserted this payload, if not a
    *     [[SavePayloadsError.ConflictingPayloadId]] error will be returned.
    *   - Finally we filter to payloads that haven't yet been successfully inserted and go back to
    *     the first step attempting to reinsert just this subset.
    */
  private def savePayloadsResolvingConflicts(
      payloads: NonEmpty[Seq[BytesPayload]],
      instanceDiscriminator: UUID,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] = {

    // insert the provided payloads with the associated discriminator to the payload table.
    // we're intentionally using a insert that will fail with a primary key constraint violation if rows exist
    def insert(payloadsToInsert: NonEmpty[Seq[BytesPayload]]): FutureUnlessShutdown[Boolean] = {
      def isConstraintViolation(batchUpdateException: SQLException): Boolean = profile match {
        case Postgres(_) => batchUpdateException.getSQLState == PSQLState.UNIQUE_VIOLATION.getState
        case H2(_) => batchUpdateException.getSQLState == H2ErrorCode.DUPLICATE_KEY_1.toString
      }

      // batch update exceptions can chain multiple exceptions for potentially each query
      // only kick in to this retry if we only see constraint violations and nothing more severe
      @tailrec
      def areAllConstraintViolations(batchUpdateException: SQLException): Boolean =
        if (!isConstraintViolation(batchUpdateException)) false
        else {
          // this one is, but are the rest?
          val nextException = batchUpdateException.getNextException

          if (nextException == null) true // no more
          else areAllConstraintViolations(nextException)
        }

      val insertSql =
        "insert into sequencer_payloads (id, instance_discriminator, content) values (?, ?, ?)"

      storage
        .queryAndUpdate(
          DbStorage.bulkOperation(insertSql, payloadsToInsert, storage.profile) { pp => payload =>
            pp >> payload.id.unwrap
            pp >> instanceDiscriminator
            pp >> payload.content
          },
          functionFullName,
        )
        .transform {
          // we would typically expect a constraint violation to be thrown if there is a conflict
          // however we double check here that each command returned a updated row and will double check if not
          case Success(UnlessShutdown.Outcome(rowCounts)) =>
            val allRowsInserted = rowCounts.forall(_ > 0)
            Success(UnlessShutdown.Outcome(allRowsInserted))

          case Success(UnlessShutdown.AbortedDueToShutdown) =>
            Success(UnlessShutdown.AbortedDueToShutdown)

          // if the only exceptions we received are constraint violations then just check and maybe try again
          case Failure(batchUpdateException: java.sql.BatchUpdateException)
              if areAllConstraintViolations(batchUpdateException) =>
            Success(UnlessShutdown.Outcome(false))

          case Failure(otherThrowable) => Failure(otherThrowable)
        }
    }

    // work out from the provided set of payloads that we attempted to insert which are now stored successfully
    // and which are still missing.
    // will return an error if the payload exists but with a different uniquifier as this suggests another process
    // has inserted a conflicting value.
    def listMissing(): EitherT[FutureUnlessShutdown, SavePayloadsError, Seq[BytesPayload]] = {
      val payloadIds = payloads.map(_.id)
      val query =
        (sql"select id, instance_discriminator from sequencer_payloads where " ++ DbStorage
          .toInClause("id", payloadIds))
          .as[(PayloadId, UUID)]

      for {
        inserted <- EitherT.right {
          storage.query(query, functionFullName)
        } map (_.toMap)
        // take all payloads we were expecting and then look up from inserted whether they are present and if they have
        // a matching instance discriminator (meaning we put them there)
        missing <- payloads.toNEF
          .foldM(Seq.empty[BytesPayload]) { (missing, payload) =>
            inserted
              .get(payload.id)
              .fold[Either[SavePayloadsError, Seq[BytesPayload]]](Right(missing :+ payload)) {
                storedDiscriminator =>
                  // we expect the local and stored instance discriminators should match otherwise it suggests the payload
                  // was inserted by another `savePayloads` call
                  Either.cond(
                    storedDiscriminator == instanceDiscriminator,
                    missing,
                    SavePayloadsError.ConflictingPayloadId(payload.id, storedDiscriminator),
                  )
              }
          }
          .toEitherT[FutureUnlessShutdown]
      } yield missing
    }

    def go(
        remainingPayloadsToInsert: NonEmpty[Seq[BytesPayload]]
    ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] =
      EitherT
        .right(insert(remainingPayloadsToInsert))
        .flatMap { successful =>
          if (!successful) listMissing()
          else EitherT.pure[FutureUnlessShutdown, SavePayloadsError](Seq.empty[BytesPayload])
        }
        .flatMap { missing =>
          // do we have any remaining to insert
          NonEmpty.from(missing).fold(EitherTUtil.unitUS[SavePayloadsError]) { missing =>
            logger.debug(
              s"Retrying to insert ${missing.size} missing of ${remainingPayloadsToInsert.size} payloads"
            )
            go(missing)
          }
        }

    go(payloads)
  }

  override def savePayloads(payloads: NonEmpty[Seq[BytesPayload]], instanceDiscriminator: UUID)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] =
    if (blockSequencerMode) {
      savePayloadsWithDiscriminator(payloads, instanceDiscriminator)
    } else {
      savePayloadsResolvingConflicts(payloads, instanceDiscriminator)
    }

  override def saveEvents(instanceIndex: Int, events: NonEmpty[Seq[Sequenced[PayloadId]]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val saveEventSql =
      """insert into sequencer_events (
        |  ts, node_index, event_type, message_id, sender, recipients,
        |  payload_id, topology_timestamp, trace_context, error, consumed_cost, extra_traffic_consumed, base_traffic_remainder
        |)
        |  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |  on conflict do nothing""".stripMargin

    val eventRows = events.map(DeliverStoreEventRow(instanceIndex, _))

    val saveEventsAction = DbStorage.bulkOperation_(saveEventSql, eventRows, storage.profile) {
      pp => eventRow =>
        val DeliverStoreEventRow(
          timestamp,
          sequencerInstanceIndex,
          eventType,
          messageId,
          sender,
          recipients,
          payloadId,
          topologyTimestampO,
          traceContext,
          errorO,
          trafficReceiptO,
        ) = eventRow

        pp >> timestamp
        pp >> sequencerInstanceIndex
        pp >> eventType
        pp >> messageId
        pp >> sender
        pp >> recipients
        pp >> payloadId
        pp >> topologyTimestampO
        pp >> SerializableTraceContext(traceContext)
        pp >> errorO
        pp >> trafficReceiptO.map(_.consumedCost)
        pp >> trafficReceiptO.map(_.extraTrafficConsumed)
        pp >> trafficReceiptO.map(_.baseTrafficRemainder)
    }

    val saveEventRecipientsSql =
      """insert into sequencer_event_recipients (node_index, recipient_id, ts, is_topology_event)
        |values (?, ?, ?, ?)
        |on conflict do nothing""".stripMargin

    for {
      sequencerMemberId <- lookupMember(sequencerMember)
        .map(
          _.map(_.memberId)
            .getOrElse(
              ErrorUtil.invalidState(
                s"Sequencer member $sequencerMember not found in sequencer members table"
              )
            )
        )
      recipientRows = eventRows.forgetNE.flatMap { row =>
        row.recipientsO.toList.flatMap { members =>
          val isTopologyEvent =
            members.contains(sequencerMemberId) && members.sizeIs > 1
          members.map(m => (row.instanceIndex, m, row.timestamp, isTopologyEvent))
        }
      }

      saveRecipientsAction = DbStorage.bulkOperation_(
        saveEventRecipientsSql,
        recipientRows,
        storage.profile,
      ) { pp => row =>
        val (sequencerInstanceIndex, recipient, timestamp, isTopologyEvent) = row
        pp >> sequencerInstanceIndex
        pp >> recipient
        pp >> timestamp
        pp >> isTopologyEvent
      }
      _ <- storage.queryAndUpdate(
        DBIO.seq(saveEventsAction, saveRecipientsAction).transactionally,
        functionFullName,
      )
    } yield ()
  }

  override def resetWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] = {

    def updateAction(): DBIOAction[Int, NoStream, Effect.Write & Effect.Transactional] =
      profile match {
        case _: Postgres =>
          sqlu"""update sequencer_watermarks
                  set watermark_ts = $ts, sequencer_online = ${false}
                  where
                    node_index = $instanceIndex and
                    watermark_ts >= $ts
               """
        case _: H2 =>
          sqlu"""merge into sequencer_watermarks using dual
                  on (node_index = $instanceIndex)
                  when matched and watermark_ts >= $ts then
                    update set watermark_ts = $ts, sequencer_online = ${false}
                """
      }

    for {
      _ <- EitherT.right(storage.update(updateAction(), functionFullName))
      updatedWatermarkO <- EitherT.right(fetchWatermark(instanceIndex))
    } yield {
      if (updatedWatermarkO.forall(w => w.online || w.timestamp != ts)) {
        logger.debug(
          s"Watermark was not reset to $ts as it is already set to an earlier date, kept $updatedWatermarkO"
        )
      }
    }
  }

  override def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] = {
    import SaveWatermarkError.*

    def save: DBIOAction[Int, NoStream, Effect.Write & Effect.Transactional] =
      profile match {
        case _: Postgres =>
          sqlu"""insert into sequencer_watermarks (node_index, watermark_ts, sequencer_online)
               values ($instanceIndex, $ts, true)
               on conflict (node_index) do
                 update set watermark_ts = $ts where sequencer_watermarks.sequencer_online = true
               """
        case _: H2 =>
          sqlu"""merge into sequencer_watermarks using dual
                  on (node_index = $instanceIndex)
                  when matched and sequencer_online = ${true} then
                    update set watermark_ts = $ts
                  when not matched then
                    insert (node_index, watermark_ts, sequencer_online) values ($instanceIndex, $ts, ${true})
                """
      }

    for {
      _ <- EitherT.right(storage.update(save, functionFullName))
      updatedWatermarkO <- EitherT.right(fetchWatermark(instanceIndex))
      // we should have just inserted or updated a watermark, so should certainly exist
      updatedWatermark <- EitherT.fromEither[FutureUnlessShutdown](
        updatedWatermarkO.toRight(
          WatermarkUnexpectedlyChanged("The watermark we should have written has been removed")
        )
      )
      // check we're still online
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](updatedWatermark.online, WatermarkFlaggedOffline)
      // check the timestamp is what we've just written.
      // if not it implies that another sequencer instance is writing with the same node index, most likely due to a
      // configuration error.
      // note we'd only observe this if the other sequencer watermark update is interleaved with ours
      // which drastically limits the possibility of observing this mis-configuration scenario.
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          updatedWatermark.timestamp == ts,
          WatermarkUnexpectedlyChanged(s"We wrote $ts but read back ${updatedWatermark.timestamp}"),
        )
        .leftWiden[SaveWatermarkError]
    } yield ()
  }

  override def fetchWatermark(
      instanceIndex: Int,
      maxRetries: Int = retry.Forever,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[Watermark]] =
    storage
      .querySingle(
        {
          val query =
            sql"""select watermark_ts, sequencer_online
                    from sequencer_watermarks
                    where node_index = $instanceIndex"""
          def watermark(row: (CantonTimestamp, Boolean)) = Watermark(row._1, row._2)
          query.as[(CantonTimestamp, Boolean)].headOption.map(_.map(watermark))
        },
        functionFullName,
        maxRetries,
      )
      .value

  override def goOffline(
      instanceIndex: Int
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    CloseContext.withCombinedContext(callerCloseContext, this.closeContext, timeouts, logger) {
      cc =>
        val action =
          sqlu"update sequencer_watermarks set sequencer_online = false where node_index = $instanceIndex"
            .withStatementParameters(statementInit =
              // We set a timeout to avoid this query taking too long. This makes sense here as this method is run when
              // the sequencer is marking itself as offline (e.g during shutdown). In such case we don't want to wait too long
              // and potentially timing out the shutdown procedure for this.
              _.setQueryTimeout(
                storage.dbConfig.parameters.connectionTimeout.toInternal
                  .toSecondsTruncated(logger)
                  .unwrap
              )
            )

        storage
          .update_(action, functionFullName)(traceContext, cc)
          .recover { case _: TimeoutException =>
            logger.debug(s"goOffline of instance $instanceIndex timed out")
            if (cc.context.isClosing) UnlessShutdown.AbortedDueToShutdown else UnlessShutdown.unit
          }
    }

  override def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    storage.queryAndUpdate(
      {
        val lookupMaxAndUpdate = for {
          maxExistingWatermark <- sql"select max(watermark_ts) from sequencer_watermarks"
            .as[Option[CantonTimestamp]]
            .headOption
          watermark = maxExistingWatermark.flatten.map(_ max now).getOrElse(now)
          _ <- profile match {
            case _: Postgres =>
              sqlu"""insert into sequencer_watermarks (node_index, watermark_ts, sequencer_online)
               values ($instanceIndex, $watermark, true)
               on conflict (node_index) do
                 update set watermark_ts = $watermark, sequencer_online = true
               """
            case _: H2 =>
              sqlu"""merge into sequencer_watermarks using dual
                  on (node_index = $instanceIndex)
                  when matched then
                    update set watermark_ts = $watermark, sequencer_online = 1
                  when not matched then
                    insert (node_index, watermark_ts, sequencer_online) values($instanceIndex, $watermark, ${true})
                """
          }
        } yield watermark

        // ensure that a later watermark won't be inserted between when we query the max watermark and set ours
        lookupMaxAndUpdate.transactionally.withTransactionIsolation(
          TransactionIsolation.Serializable
        )
      },
      functionFullName,
    )

  override def fetchOnlineInstances(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedSet[Int]] =
    storage
      .query(
        sql"select node_index from sequencer_watermarks where sequencer_online = ${true}".as[Int],
        functionFullName,
      )
      .map(items => SortedSet(items*))

  override def readPayloads(payloadIds: Seq[IdOrPayload], member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Batch[ClosedEnvelope]]] = {

    val preloadedPayloads = payloadIds.collect {
      case payload: BytesPayload =>
        payload.id -> payload.decodeBatchAndTrim(protocolVersion, member)
      case batch: FilteredBatch => batch.id -> Batch.trimForMember(batch.batch, member)
    }.toMap

    val idsToLoad = payloadIds.collect { case id: PayloadId => id }

    logger.debug(
      s"readPayloads: reusing buffered ${preloadedPayloads.size} payloads and requesting ${idsToLoad.size}"
    )

    payloadCache.getAll(idsToLoad).map { accessedPayloads =>
      preloadedPayloads ++ accessedPayloads.view
        .mapValues(_.decodeBatchAndTrim(protocolVersion, member))
    }
  }

  private def readPayloadsFromStore(payloadIds: Seq[PayloadId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, BytesPayload]] =
    NonEmpty.from(payloadIds) match {
      case None => FutureUnlessShutdown.pure(Map.empty)
      case Some(payloadIdsNE) =>
        val query = sql"""select id, content
        from sequencer_payloads
        where """ ++ DbStorage.toInClause("id", payloadIdsNE)
        storage.query(query.as[BytesPayload], functionFullName).map { payloads =>
          payloads.map(p => p.id -> p).toMap
        }
    }

  override protected def readEventsInternal(
      memberId: SequencerMemberId,
      fromTimestampExclusiveO: Option[CantonTimestamp],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ReadEvents] = {
    // fromTimestampO is an exclusive lower bound if set
    // to make inclusive we add a microsecond (the smallest unit)
    // this comparison can then be used for the absolute lower bound if unset
    val fromTimestampInclusive =
      fromTimestampExclusiveO.map(_.immediateSuccessor).getOrElse(CantonTimestamp.MinValue)

    def h2PostgresQueryEvents(
        memberContainsBefore: String,
        memberContainsAfter: String,
        safeWatermark: CantonTimestamp,
    ) = sql"""
        select events.ts, events.node_index, events.event_type, events.message_id, events.sender,
          events.recipients, events.payload_id, events.topology_timestamp,
          events.trace_context, events.error,
          events.consumed_cost, events.extra_traffic_consumed, events.base_traffic_remainder
        from sequencer_events events
        inner join sequencer_watermarks watermarks
          on events.node_index = watermarks.node_index
        where (events.recipients is null or (#$memberContainsBefore $memberId #$memberContainsAfter))
          and (
            -- inclusive timestamp bound that defaults to MinValue if unset
            events.ts >= $fromTimestampInclusive
              -- only consider events within the safe watermark
              and events.ts <= $safeWatermark
              -- if the sequencer that produced the event is offline, only consider up until its offline watermark
              and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
          )
        order by events.ts asc
        limit $limit"""

    def queryEvents(safeWatermarkO: Option[CantonTimestamp]) = {
      // If we don't have a safe watermark of all online sequencers (if all are offline) we'll fallback on allowing all
      // and letting the offline condition in the query include the event if suitable
      val safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
      val query = profile match {
        case _: Postgres =>
          h2PostgresQueryEvents("", " = any(events.recipients)", safeWatermark)

        case _: H2 =>
          h2PostgresQueryEvents("array_contains(events.recipients, ", ")", safeWatermark)
      }

      query.as[Sequenced[PayloadId]]
    }

    val query = for {
      safeWatermark <- safeWaterMarkDBIO
      events <- queryEvents(safeWatermark)
    } yield {
      (events, safeWatermark)
    }

    storage.query(query.transactionally, functionFullName).map {
      case (events, _) if events.nonEmpty => ReadEventPayloads(events)
      case (_, watermark) => SafeWatermark(watermark)
    }
  }

  private def readEventsLatest(
      limit: PositiveInt,
      upperBoundExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Vector[Sequenced[BytesPayload]]] = {
    def queryEvents(safeWatermark: CantonTimestamp) = {
      val query =
        sql"""
        select events.ts, events.node_index, events.event_type, events.message_id, events.sender,
          events.recipients, payloads.id, payloads.content, events.topology_timestamp,
          events.trace_context, events.error,
          events.consumed_cost, events.extra_traffic_consumed, events.base_traffic_remainder
        from sequencer_events events
        left join sequencer_payloads payloads
          on events.payload_id = payloads.id
        inner join sequencer_watermarks watermarks
          on events.node_index = watermarks.node_index
        where
          (
              -- only consider events within the safe watermark
              events.ts <= $safeWatermark
              and events.ts < $upperBoundExclusive
              -- if the sequencer that produced the event is offline, only consider up until its offline watermark
              and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
          )
        order by events.ts desc
        limit $limit"""

      query.as[Sequenced[BytesPayload]]
    }

    val query = for {
      safeWatermarkO <- safeWaterMarkDBIO
      // If we don't have a safe watermark of all online sequencers (if all are offline) we'll fallback on allowing all
      // and letting the offline condition in the query include the event if suitable
      safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
      events <- queryEvents(safeWatermark)
    } yield events

    storage.query(query.transactionally, functionFullName)
  }

  override protected def preloadBufferInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    eventsBuffer.invalidateBuffer()
    // Load events in to the buffer in batches, starting with the latest batch and going backwards
    // until the buffer is full
    logger.info(s"Preloading the events buffer with a memory limit of $bufferedEventsMaxMemory")

    Monad[FutureUnlessShutdown]
      .tailRecM(CantonTimestamp.MaxValue) { upperBoundExclusive =>
        logger.debug(s"Fetching events with exclusive upper bound $upperBoundExclusive")
        readEventsLatest(bufferedEventsPreloadBatchSize, upperBoundExclusive).map {
          eventsByTimestampDescending =>
            NonEmpty.from(eventsByTimestampDescending.reverse) match {
              case None =>
                // no events found, no need to try to fetch more events
                Right(())
              case Some(eventsNE) =>
                logger.debug(
                  s"Loading ${eventsNE.size} events into the buffer: first ${eventsNE.head1.timestamp}, last: ${eventsNE.last1.timestamp}"
                )
                Either.cond(
                  // prependEventsForPreloading returns true if the buffer is full
                  eventsBuffer.prependEventsForPreloading(
                    eventsNE
                  ) || eventsNE.sizeIs < bufferedEventsPreloadBatchSize.value,
                  (), // buffer is full, no need to fetch more events
                  eventsNE.head1.timestamp, // there is still room to fetch more events
                )
            }
        }
      }
      .thereafter { _ =>
        val buf = eventsBuffer.snapshot()
        val numBufferedEvents = buf.size
        val minTimestamp = buf.headOption.map(_.timestamp).getOrElse(CantonTimestamp.MinValue)
        val maxTimestamp = buf.lastOption.map(_.timestamp).getOrElse(CantonTimestamp.MaxValue)
        logger.info(s"Loaded $numBufferedEvents events between $minTimestamp and $maxTimestamp")
      }
  }

  private def safeWaterMarkDBIO: DBIOAction[Option[CantonTimestamp], NoStream, Effect.Read] = {
    val query = profile match {
      case _: H2 | _: Postgres =>
        sql"select min(watermark_ts) from sequencer_watermarks where sequencer_online = true"
    }
    // `min` may return null that is wrapped into None
    query.as[Option[CantonTimestamp]].headOption.map(_.flatten)
  }

  override def safeWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    storage.query(safeWaterMarkDBIO, "query safe watermark")

  override def readStateAtTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SequencerSnapshot] = {
    val query = for {
      safeWatermarkO <- safeWaterMarkDBIO
      previousEventTimestamps <- memberPreviousEventTimestamps(
        timestamp,
        safeWatermarkO.getOrElse(CantonTimestamp.MaxValue),
      )
    } yield previousEventTimestamps
    for {
      previousTimestampsAtTimestamps <- storage.query(
        query.transactionally,
        functionFullName,
      )
      statusAtTimestamp <- status(timestamp)
    } yield {
      SequencerSnapshot(
        timestamp,
        UninitializedBlockHeight,
        previousTimestampsAtTimestamps,
        statusAtTimestamp,
        Map.empty,
        None,
        protocolVersion,
        Seq.empty,
        Seq.empty,
      )
    }
  }

  /**   - Without filters this returns results for all enabled members, to be used in the sequencer
    *     snapshot.
    *   - With `filterForMemberO = Some(member, false)` this returns results for a specific member,
    *     to be used when reading events for the member.
    *   - With `filterForMemberO = Some(member, true)` this returns results the "candidate topology"
    *     timestamp that is safe to use in the member's subscription.
    *     - In this case, if the returned value is below the sequencer lower bound, the lower bound
    *       should be used instead.
    */
  private def memberPreviousEventTimestamps(
      beforeInclusive: CantonTimestamp,
      safeWatermark: CantonTimestamp,
      filterForMemberO: Option[(SequencerMemberId, Boolean)] = None,
  ): DBIOAction[Map[Member, Option[CantonTimestamp]], NoStream, Effect.Read] = {
    val memberFilter = filterForMemberO
      .map { case (memberId, _) =>
        sql"and id = $memberId"
      }
      .getOrElse(sql"")
    val topologyClientMemberFilter =
      if (
        filterForMemberO.exists { case (_, filterTopologyEvent) =>
          filterTopologyEvent
        }
      )
        sql"""and is_topology_event is true"""
      else sql""

    (sql"""
            with
              enabled_members as (
                select
                  member,
                  id,
                  registered_ts,
                  pruned_previous_event_timestamp
                from sequencer_members
                where
                  -- consider the given timestamp
                  registered_ts <= $beforeInclusive
                  -- no need to consider disabled members since they can't be served events anymore
                  and enabled = true
                  """ ++ memberFilter ++ sql"""
              ),
              watermarks as (
                select
                  node_index,
                  case
                    when sequencer_online then ${CantonTimestamp.MaxValue.toMicros}
                    else watermark_ts
                  end as watermark_ts
                from sequencer_watermarks
                where watermark_ts is not null
              )
           select
             m.member,
             coalesce(
               (
                 select
                   (
                     select member_recipient.ts
                     from sequencer_event_recipients member_recipient
                     where
                       member_recipient.node_index = watermarks.node_index
                       and m.id = member_recipient.recipient_id
                       """ ++ topologyClientMemberFilter ++ sql"""
                       and member_recipient.ts <= watermarks.watermark_ts
                       and member_recipient.ts <= $beforeInclusive
                       and member_recipient.ts <= $safeWatermark
                       and member_recipient.ts >= m.registered_ts
                     order by member_recipient.node_index, member_recipient.recipient_id, member_recipient.ts desc
                     limit 1
                   ) as ts
                 from watermarks
                 order by ts desc
                 limit 1
               ),
               m.pruned_previous_event_timestamp
             ) previous_ts
           from enabled_members m""").as[(Member, Option[CantonTimestamp])].map(_.toMap)
  }

  override def deleteEventsPastWatermark(
      instanceIndex: Int
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] =
    for {
      watermarkO <- storage.query(
        sql"select watermark_ts from sequencer_watermarks where node_index = $instanceIndex"
          .as[CantonTimestamp]
          .headOption,
        functionFullName,
      )
      watermark = watermarkO.getOrElse(CantonTimestamp.MinValue)
      // TODO(#18401): Also cleanup payloads (beyond the payload to event margin)
      eventsRemoved <- storage.update(
        sqlu"""
            delete from sequencer_events
            where node_index = $instanceIndex
                and ts > $watermark
           """,
        functionFullName,
      )
    } yield {
      logger.debug(
        s"Removed at least $eventsRemoved events that were past the last watermark ($watermarkO) for this sequencer"
      )
      watermarkO
    }

  override def fetchPreviousEventTimestamp(
      memberId: SequencerMemberId,
      timestampInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {

    val query = for {
      safeWatermarkO <- safeWaterMarkDBIO
      safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
      previousEventTimestamp <-
        sql"""
            select coalesce(
              ( -- first we try to find the previous event for the member
                select ts
                  from sequencer_events events
                  left join sequencer_watermarks watermarks
                          on events.node_index = watermarks.node_index
                  where
                   (
                     -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                     watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                   )
                   and ts <= $timestampInclusive
                   and (#$memberContainsBefore $memberId #$memberContainsAfter)
                   and ts <= $safeWatermark
                 order by ts desc
                 limit 1
               ),
              ( -- otherwise we fall back to the timestamp saved by pruning or onboarding
                select pruned_previous_event_timestamp
                from sequencer_members
                where id = $memberId
              )
            ) as previous_event_timestamp
             """.as[Option[CantonTimestamp]].headOption
    } yield previousEventTimestamp
    storage.query(query, functionFullName).map(_.flatten)
  }

  override def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      profile match {
        case _: Postgres =>
          sqlu"""insert into sequencer_acknowledgements (member, ts)
               values ($member, $timestamp)
               on conflict (member) do update set ts = excluded.ts where excluded.ts > sequencer_acknowledgements.ts
               """
        case _: H2 =>
          sqlu"""merge into sequencer_acknowledgements using dual
                  on member = $member
                  when matched and $timestamp > ts then
                    update set ts = $timestamp
                  when not matched then
                    insert values ($member, $timestamp)
                """
      },
      functionFullName,
    )

  override def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerMemberId, CantonTimestamp]] =
    storage
      .query(
        sql"""
                  select member, ts
                  from sequencer_acknowledgements
           """.as[(SequencerMemberId, CantonTimestamp)],
        functionFullName,
      )
      .map(_.map { case (memberId, timestamp) => memberId -> timestamp })
      .map(_.toMap)

  private def fetchLowerBoundDBIO(): ReadOnly[Option[(CantonTimestamp, Option[CantonTimestamp])]] =
    sql"select ts, latest_topology_client_timestamp from sequencer_lower_bound"
      .as[(CantonTimestamp, Option[CantonTimestamp])]
      .headOption

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(CantonTimestamp, Option[CantonTimestamp])]] =
    storage.querySingle(fetchLowerBoundDBIO(), "fetchLowerBound").value

  override def saveLowerBound(
      ts: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SaveLowerBoundError, Unit] =
    EitherT(
      storage.queryAndUpdate(
        (for {
          existingTsO <- dbEitherT(fetchLowerBoundDBIO())
          _ <- EitherT.fromEither[DBIO](
            existingTsO
              .filter { case (existingTs, existingTopologyTs) =>
                existingTs > ts || existingTopologyTs > latestTopologyClientTimestamp
              }
              .map(
                SaveLowerBoundError.BoundLowerThanExisting(_, (ts, latestTopologyClientTimestamp))
              )
              .toLeft(())
          )
          _ <- dbEitherT[SaveLowerBoundError](
            existingTsO.fold(
              sqlu"insert into sequencer_lower_bound (ts, latest_topology_client_timestamp) values ($ts, $latestTopologyClientTimestamp)"
            )(_ =>
              sqlu"update sequencer_lower_bound set ts = $ts, latest_topology_client_timestamp = $latestTopologyClientTimestamp"
            )
          )
        } yield ()).value.transactionally
          .withTransactionIsolation(TransactionIsolation.Serializable),
        "saveLowerBound",
      )
    )

  override protected def updatePrunedPreviousEventTimestampsInternal(
      updatedPreviousTimestamps: Map[SequencerMemberId, CantonTimestamp]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateSql =
      """update sequencer_members
         set pruned_previous_event_timestamp = ?
         where id = ?"""

    val bulkInsert = DbStorage
      .bulkOperation(updateSql, updatedPreviousTimestamps, storage.profile) {
        pp => idAndTimestamp =>
          val memberId -> timestamp = idAndTimestamp
          pp >> timestamp
          pp >> memberId
      }
      .map(_.sum)
    storage
      .queryAndUpdate(bulkInsert, functionFullName)
      .map(updateCount =>
        logger.debug(
          s"Updated $updateCount sequencer members with pruned previous event timestamps"
        )
      )
  }

  // TODO(#25162): Sequencer onboarding produces an inclusive lower bound (event is not exactly available at it),
  //  need to align the pruning and the onboarding definitions of the lower bound
  override protected[store] def pruneEvents(
      beforeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.update(
      DBIO
        .sequence(
          Seq(
            sqlu"delete from sequencer_events where ts < $beforeExclusive",
            sqlu"delete from sequencer_event_recipients where ts < $beforeExclusive",
          )
        )
        .map(_.sum),
      functionFullName,
    )

  override protected[store] def prunePayloads(
      beforeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.update(
      sqlu"delete from sequencer_payloads where id < $beforeExclusive",
      functionFullName,
    )

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = storage
    .querySingle(
      sql"""select ts from sequencer_events order by ts #${storage.limit(
          1,
          skipItems = skip.value.toLong,
        )}""".as[CantonTimestamp].headOption,
      functionFullName,
    )
    .value

  override def status(
      now: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SequencerPruningStatus] =
    for {
      lowerBoundO <- fetchLowerBound()
      members <- storage.query(
        sql"""
      select member, id, registered_ts, enabled from sequencer_members where registered_ts <= $now"""
          .as[(Member, SequencerMemberId, CantonTimestamp, Boolean)],
        functionFullName,
      )
      acknowledgements <- latestAcknowledgements()
    } yield {
      SequencerPruningStatus(
        lowerBound =
          lowerBoundO.map { case (timestamp, _) => timestamp }.getOrElse(CantonTimestamp.Epoch),
        now = now,
        members = members.view.map { case (member, memberId, registeredAt, enabled) =>
          SequencerMemberStatus(
            member,
            registeredAt,
            lastAcknowledged = acknowledgements.get(memberId),
            enabled = enabled,
          )
        }.toSet,
      )
    }

  override def markLaggingSequencersOffline(
      cutoffTime: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      rowsUpdated <- storage.update(
        sqlu"""update sequencer_watermarks
                set sequencer_online = ${false}
                where sequencer_online = ${true} and watermark_ts <= $cutoffTime""",
        functionFullName,
      )
    } yield {
      if (rowsUpdated > 0) {
        // The log message may be omitted if `update` underreports the number of changed rows
        logger.info(
          s"Knocked $rowsUpdated sequencers offline that haven't been active since cutoff of $cutoffTime"
        )
      }
    }

  @VisibleForTesting
  override protected[canton] def countRecords(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerStoreRecordCounts] = {
    def count(statement: canton.SQLActionBuilder): FutureUnlessShutdown[Long] =
      storage.query(statement.as[Long].head, functionFullName)

    for {
      events <- count(sql"select count(*) from sequencer_events")
      eventRecipients <- count(sql"select count(*) from sequencer_event_recipients")
      payloads <- count(sql"select count(*) from sequencer_payloads")
    } yield SequencerStoreRecordCounts(events, eventRecipients, payloads)
  }

  /** Count stored events for this node. Used exclusively by tests. */
  @VisibleForTesting
  private[store] def countEventsForNode(
      instanceIndex: Int
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.query(
      sql"select count(ts) from sequencer_events where node_index = $instanceIndex".as[Int].head,
      functionFullName,
    )

  override protected def disableMemberInternal(member: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    // we assume here that the member is already registered in order to have looked up the memberId
    storage.update_(
      sqlu"update sequencer_members set enabled = ${false} where id = $member",
      functionFullName,
    )

  override def validateCommitMode(
      configuredCommitMode: CommitMode
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val stringReader = GetResult.GetString
    storage.profile match {
      case H2(_) =>
        // we don't worry about replicas or commit modes in h2
        EitherTUtil.unitUS
      case Postgres(_) =>
        for {
          settingO <- EitherT.right(
            storage.query(
              sql"select setting from pg_settings where name = 'synchronous_commit'"
                .as[String](stringReader)
                .headOption,
              functionFullName,
            )
          )
          setting <- settingO
            .toRight(
              s"""|Setting for 'synchronous_commit' appears to be unset when validating the current commit mode.
                  |Either validate your postgres configuration,
                  | or if you are confident with your configuration then disable commit mode validation.""".stripMargin
            )
            .toEitherT[FutureUnlessShutdown]
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            configuredCommitMode.postgresSettings.toList.contains(setting),
            s"Postgres 'synchronous_commit' setting is '$setting' but expecting one of: ${configuredCommitMode.postgresSettings.toList
                .mkString(",")}",
          )
        } yield ()
    }
  }

  /** For a given member and timestamp, return the latest timestamp of a potential topology change,
    * that reached both the sequencer and the member. To be used by the topology snapshot awaiting,
    * should there be a topology change expected to need to be taken into account for
    * `timestampExclusive` sequencing timestamp.
    */
  override def latestTopologyClientRecipientTimestamp(
      member: Member,
      timestampExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {

    def query(registeredMember: RegisteredMember) = for {
      safeWatermarkO <- safeWaterMarkDBIO
      membersPreviousTimestamps <- memberPreviousEventTimestamps(
        beforeInclusive = timestampExclusive.immediatePredecessor,
        safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue),
        filterForMemberO = Some(registeredMember.memberId -> true),
      )
    } yield {
      membersPreviousTimestamps.headOption.flatMap { case (_, ts) => ts }
    }

    for {
      registeredMember <- lookupMember(member).map(
        _.getOrElse(
          ErrorUtil.invalidState(
            s"Member $member not found in sequencer members table"
          )
        )
      )
      lowerBoundO <- fetchLowerBound()
      _ = logger.debug(
        s"Sequencer lower bound is $lowerBoundO"
      )
      // Here we look for an event that reached both the sequencer and the member,
      // because that's how we generate the latest topology client timestamp during a running subscription.
      // If no such event found the query will return sequencer_members.pruned_previous_event_timestamp,
      // which will be below the lower bound or be None.
      latestTopologyTimestampCandidate <- storage.query(
        query(registeredMember),
        functionFullName,
      )
    } yield {
      lowerBoundO match {
        // If a lower bound is set (pruned or onboarded sequencer),
        // and we didn't find any event that reached both the sequencer and the member,
        // or we found one, but it is below or at the lower bound
        case Some((lowerBound, topologyClientLowerBound))
            if latestTopologyTimestampCandidate.forall(_ <= lowerBound) =>
          // Then we use the topology client timestamp at the lower bound
          topologyClientLowerBound
        // In other cases:
        // - If there's no lower bound
        // - If there's a lower bound, and found an event above the lower bound
        //   that reached both the sequencer and the member
        case _ =>
          // We use the looked up event, falling back to the member's registration time
          Some(latestTopologyTimestampCandidate.getOrElse(registeredMember.registeredFrom))
      }
    }
  }

  /** For a given member find the timestamp of the last event that the member has received before
    * `timestampExclusive`.
    */
  override def previousEventTimestamp(
      memberId: SequencerMemberId,
      timestampExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val query = for {
      safeWatermarkO <- safeWaterMarkDBIO
      previousTimestamp <- memberPreviousEventTimestamps(
        beforeInclusive = timestampExclusive.immediatePredecessor,
        safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue),
        filterForMemberO = Some(memberId -> false),
      )
    } yield previousTimestamp

    storage.query(query, functionFullName).map(_.headOption.flatMap(_._2))
  }
}
