// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import cats.data.EitherT
import cats.implicits.catsSyntaxOrder
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.catsinstances.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
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
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.db.RequiredTypesCodec.*
import com.digitalasset.canton.synchronizer.block.UninitializedBlockHeight
import com.digitalasset.canton.synchronizer.sequencing.sequencer.{
  CommitMode,
  SequencerMemberStatus,
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, retry}
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
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}

/** Database backed sequencer store.
  * Supports many concurrent instances reading and writing to the same backing database.
  */
class DbSequencerStore(
    @VisibleForTesting private[canton] val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    maxBufferedEventsSize: NonNegativeInt,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    sequencerMember: Member,
    override val blockSequencerMode: Boolean,
    cachingConfigs: CachingConfigs,
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

  private implicit val getPayloadResult: GetResult[Payload] =
    GetResult
      .createGetTuple2[PayloadId, ByteString]
      .andThen { case (id, content) =>
        Payload(id, content)
      }

  /** @param trafficReceiptO If traffic management is enabled, there should always be traffic information for the sender.
    *                        The information might be discarded later though, in case the event is being processed as part
    *                        of a subscription for any of the recipients that isn't the sender.
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

  private implicit val getPayloadOResult: GetResult[Option[Payload]] =
    GetResult
      .createGetTuple2[Option[PayloadId], Option[ByteString]]
      .andThen {
        case (Some(id), Some(content)) => Some(Payload(id, content))
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

  private implicit val getDeliverStoreEventRowResultWithPayload: GetResult[Sequenced[Payload]] = {
    val timestampGetter = implicitly[GetResult[CantonTimestamp]]
    val timestampOGetter = implicitly[GetResult[Option[CantonTimestamp]]]
    val discriminatorGetter = implicitly[GetResult[EventTypeDiscriminator]]
    val messageIdGetter = implicitly[GetResult[Option[MessageId]]]
    val memberIdGetter = implicitly[GetResult[Option[SequencerMemberId]]]
    val memberIdNesGetter = implicitly[GetResult[Option[NonEmpty[SortedSet[SequencerMemberId]]]]]
    val payloadGetter = implicitly[GetResult[Option[Payload]]]
    val traceContextGetter = implicitly[GetResult[SerializableTraceContext]]
    val errorOGetter = implicitly[GetResult[Option[ByteString]]]
    val trafficReceipt = implicitly[GetResult[Option[TrafficReceipt]]]

    GetResult { r =>
      val row = DeliverStoreEventRow[Payload](
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

  override val maxBufferedEvents: Int = maxBufferedEventsSize.unwrap

  private val profile = storage.profile

  private val (memberContainsBefore, memberContainsAfter): (String, String) = profile match {
    case _: Postgres =>
      ("", " = any(events.recipients)")
    case _: H2 =>
      ("array_contains(events.recipients, ", ")")
  }

  private val payloadCache: TracedAsyncLoadingCache[FutureUnlessShutdown, PayloadId, Payload] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, PayloadId, Payload](
      cache = cachingConfigs.sequencerPayloadCache.buildScaffeine(),
      loader = implicit traceContext =>
        payloadId => readPayloadsFromStore(Seq(payloadId)).map(_(payloadId)),
      allLoader =
        Some(implicit traceContext => payloadIds => readPayloadsFromStore(payloadIds.toSeq)),
    )(logger, "payloadCache")

  override def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerMemberId] =
    storage.queryAndUpdateUnlessShutdown(
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
    storage.queryUnlessShutdown(
      sql"""select id, registered_ts, enabled from sequencer_members where member = $member"""
        .as[(SequencerMemberId, CantonTimestamp, Boolean)]
        .headOption
        .map(_.map((RegisteredMember.apply _).tupled)),
      functionFullName,
    )

  /** In unified sequencer payload ids are deterministic (these are sequencing times from the block sequencer),
    * so we can somewhat safely ignore conflicts arising from sequencer restarts, crash recovery, ha lock loses,
    * unlike the complicate `savePayloads` method below
    */
  private def savePayloadsUS(payloads: NonEmpty[Seq[Payload]], instanceDiscriminator: UUID)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] = {
    val insertSql =
      """insert into sequencer_payloads (id, instance_discriminator, content) values (?, ?, ?) on conflict do nothing"""

    EitherT.right[SavePayloadsError](
      storage
        .queryAndUpdateUnlessShutdown(
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
    * For DB implementations we suspect that this will be a hot spot for performance primarily due to size of the payload
    * content values being inserted. To help with this we use `storage.queryAndUpdateUnsafe` to do these inserts using
    * the full connection pool of the node instead of the single connection that is protected with an exclusive lock in
    * the HA sequencer setup.
    *
    * The downside of this optimization is that if a HA writer was to lose its lock, writes for payloads will continue
    * regardless for a period until it shuts down. During this time another writer with the same instance index
    * could come online.
    * As we generate the payload id using a value generated for a partition based on the instance
    * index this could worse case mean that two sequencer writers attempt to insert different payloads with the same
    * id. If we use a simple idempotency method of just ignoring conflicts this could result in the active sequencer
    * writing an event with a different payload resulting in a horrid corruption problem. This will admittedly be difficult
    * to hit, but possible all the same.
    *
    * The approach we use here is to generate an ephemeral instance discriminator for each writer instance.
    *  - We insert the payloads using a simple insert (intentionally not ignoring conflicts unlike our other idempotent inserts).
    *    If this was successful we end here.
    *  - If this insert hits a payload with the same id the query will blow up with a unique constraint violation exception.
    *    Now there's a couple of reasons this may have happened:
    *      1. Another instance is running and has inserted a conflicting payload (bad!)
    *      2. There was some connection issue when running this query and our storage layer didn't see a successful result
    *         so it retried our query unaware that it did actually succeed so now we conflicting with our own insert.
    *         (a bit of a shame but not really a problem).
    *  - We now query filtered on the payload ids we're attempting to insert to select out which payloads exist and their
    *    respective discriminators.
    *    For any payloads that exist we check that the discriminators indicate that we inserted this payload, if not a
    *    [[SavePayloadsError.ConflictingPayloadId]] error will be returned.
    *  - Finally we filter to payloads that haven't yet been successfully inserted and go back to the first step attempting
    *    to reinsert just this subset.
    */
  private def savePayloadsResolvingConflicts(
      payloads: NonEmpty[Seq[Payload]],
      instanceDiscriminator: UUID,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] = {

    // insert the provided payloads with the associated discriminator to the payload table.
    // we're intentionally using a insert that will fail with a primary key constraint violation if rows exist
    def insert(payloadsToInsert: NonEmpty[Seq[Payload]]): FutureUnlessShutdown[Boolean] = {
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
        .queryAndUpdateUnlessShutdown(
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
    def listMissing(): EitherT[FutureUnlessShutdown, SavePayloadsError, Seq[Payload]] = {
      val payloadIds = payloads.map(_.id)
      val query =
        (sql"select id, instance_discriminator from sequencer_payloads where " ++ DbStorage
          .toInClause("id", payloadIds))
          .as[(PayloadId, UUID)]

      for {
        inserted <- EitherT.right {
          storage.queryUnlessShutdown(query, functionFullName)
        } map (_.toMap)
        // take all payloads we were expecting and then look up from inserted whether they are present and if they have
        // a matching instance discriminator (meaning we put them there)
        missing <- payloads.toNEF
          .foldM(Seq.empty[Payload]) { (missing, payload) =>
            inserted
              .get(payload.id)
              .fold[Either[SavePayloadsError, Seq[Payload]]](Right(missing :+ payload)) {
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
        remainingPayloadsToInsert: NonEmpty[Seq[Payload]]
    ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] =
      EitherT
        .right(insert(remainingPayloadsToInsert))
        .flatMap { successful =>
          if (!successful) listMissing()
          else EitherT.pure[FutureUnlessShutdown, SavePayloadsError](Seq.empty[Payload])
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

  override def savePayloads(payloads: NonEmpty[Seq[Payload]], instanceDiscriminator: UUID)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] =
    if (blockSequencerMode) {
      savePayloadsUS(payloads, instanceDiscriminator)
    } else {
      savePayloadsResolvingConflicts(payloads, instanceDiscriminator)
    }

  override def saveEvents(instanceIndex: Int, events: NonEmpty[Seq[Sequenced[PayloadId]]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val saveSql =
      """insert into sequencer_events (
        |  ts, node_index, event_type, message_id, sender, recipients,
        |  payload_id, topology_timestamp, trace_context, error, consumed_cost, extra_traffic_consumed, base_traffic_remainder
        |)
        |  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |  on conflict do nothing""".stripMargin

    storage.queryAndUpdateUnlessShutdown(
      DbStorage.bulkOperation_(saveSql, events, storage.profile) { pp => event =>
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
        ) = DeliverStoreEventRow(instanceIndex, event)

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
      },
      functionFullName,
    )
  }

  override def resetWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] = {
    import SaveWatermarkError.*

    def save: DBIOAction[Int, NoStream, Effect.Write with Effect.Transactional] =
      profile match {
        case _: Postgres =>
          sqlu"""insert into sequencer_watermarks (node_index, watermark_ts, sequencer_online)
               values ($instanceIndex, $ts, false)
               on conflict (node_index) do
                 update set watermark_ts = $ts, sequencer_online = ${false}
                 where sequencer_watermarks.watermark_ts >= $ts
               """
        case _: H2 =>
          sqlu"""merge into sequencer_watermarks using dual
                  on (node_index = $instanceIndex)
                  when matched and watermark_ts >= $ts then
                    update set watermark_ts = $ts, sequencer_online = ${false}
                  when not matched then
                    insert (node_index, watermark_ts, sequencer_online) values ($instanceIndex, $ts, ${false})
                """
      }

    for {
      _ <- EitherT.right(storage.updateUnlessShutdown(save, functionFullName))
      updatedWatermarkO <- EitherT.right(fetchWatermark(instanceIndex))
      // we should have just inserted or updated a watermark, so should certainly exist
      updatedWatermark <- EitherT.fromEither[FutureUnlessShutdown](
        updatedWatermarkO.toRight(
          WatermarkUnexpectedlyChanged("The watermark we should have written has been removed")
        )
      ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Watermark]
      _ = {
        if (updatedWatermark.online || updatedWatermark.timestamp != ts) {
          logger.debug(
            s"Watermark was not reset to $ts as it is already set to an earlier date, kept $updatedWatermark"
          )
        }
      }
    } yield ()
  }

  override def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] = {
    import SaveWatermarkError.*

    def save: DBIOAction[Int, NoStream, Effect.Write with Effect.Transactional] =
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
      _ <- EitherT.right(storage.updateUnlessShutdown(save, functionFullName))
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
      .querySingleUnlessShutdown(
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
          .updateUnlessShutdown_(action, functionFullName)(traceContext, cc)
          .recover { case _: TimeoutException =>
            logger.debug(s"goOffline of instance $instanceIndex timed out")
            if (cc.context.isClosing) UnlessShutdown.AbortedDueToShutdown else UnlessShutdown.unit
          }
    }

  override def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    storage.queryAndUpdateUnlessShutdown(
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
      .queryUnlessShutdown(
        sql"select node_index from sequencer_watermarks where sequencer_online = ${true}".as[Int],
        functionFullName,
      )
      .map(items => SortedSet(items*))

  override def readPayloads(payloadIds: Seq[IdOrPayload])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Payload]] = {

    val preloadedPayloads = payloadIds.collect { case payload: Payload =>
      payload.id -> payload
    }.toMap
    val idsToLoad = payloadIds.collect { case id: PayloadId => id }

    logger.debug(
      s"readPayloads: reusing buffered ${preloadedPayloads.size} payloads and requesting ${idsToLoad.size}"
    )
    payloadCache.getAll(idsToLoad).map { accessedPayloads =>
      accessedPayloads ++ preloadedPayloads
    }
  }

  private def readPayloadsFromStore(payloadIds: Seq[PayloadId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Payload]] =
    NonEmpty.from(payloadIds) match {
      case None => FutureUnlessShutdown.pure(Map.empty)
      case Some(payloadIdsNE) =>
        val query = sql"""select id, content
        from sequencer_payloads
        where """ ++ DbStorage.toInClause("id", payloadIdsNE)
        storage.queryUnlessShutdown(query.as[Payload], functionFullName).map { payloads =>
          payloads.map(p => p.id -> p).toMap
        }
    }

  override protected def readEventsInternal(
      memberId: SequencerMemberId,
      fromTimestampO: Option[CantonTimestamp],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ReadEvents] = {
    // fromTimestampO is an exclusive lower bound if set
    // to make inclusive we add a microsecond (the smallest unit)
    // this comparison can then be used for the absolute lower bound if unset
    val inclusiveFromTimestamp =
      fromTimestampO.map(_.immediateSuccessor).getOrElse(CantonTimestamp.MinValue)

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
            events.ts >= $inclusiveFromTimestamp
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

    storage.queryUnlessShutdown(query.transactionally, functionFullName).map {
      case (events, _) if events.nonEmpty => ReadEventPayloads(events)
      case (_, watermark) => SafeWatermark(watermark)
    }
  }

  private def readEventsLatest(
      limit: Int
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Vector[Sequenced[Payload]]] = {

    def queryEvents(safeWatermarkO: Option[CantonTimestamp]) = {
      // If we don't have a safe watermark of all online sequencers (if all are offline) we'll fallback on allowing all
      // and letting the offline condition in the query include the event if suitable
      val safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
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
              -- if the sequencer that produced the event is offline, only consider up until its offline watermark
              and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
          )
        order by events.ts desc
        limit $limit"""

      query.as[Sequenced[Payload]]
    }

    val query = for {
      safeWatermark <- safeWaterMarkDBIO
      events <- queryEvents(safeWatermark)
    } yield events

    storage.queryUnlessShutdown(query.transactionally, functionFullName)
  }

  override def prePopulateBuffer(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (maxBufferedEvents == 0) {
      logger.debug("Buffer pre-population disabled")
      FutureUnlessShutdown.unit
    } else {
      // When buffer is it capacity, we will half it. There's no point in pre-loading full buffer
      // to half it immediately afterwards.
      val populateWithCount = maxBufferedEvents / 2
      logger.debug(s"Pre-populating buffer with at most $populateWithCount events")
      readEventsLatest(populateWithCount).map { events =>
        logger.debug(s"Fan out buffer now contains ${events.size} events")
        setBuffer(events)
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
    storage.queryUnlessShutdown(safeWaterMarkDBIO, "query safe watermark")

  override def readStateAtTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SequencerSnapshot] = {
    val query = for {
      safeWatermarkO <- safeWaterMarkDBIO
      checkpoints <- memberCheckpointsQuery(
        timestamp,
        safeWatermarkO.getOrElse(CantonTimestamp.MaxValue),
      )
    } yield checkpoints
    for {
      checkpointsAtTimestamp <- storage.queryUnlessShutdown(query.transactionally, functionFullName)
      lastTs = checkpointsAtTimestamp
        .map(_._2.timestamp)
        .maxOption
        .getOrElse(CantonTimestamp.MinValue)
      statusAtTimestamp <- status(lastTs)
    } yield {
      SequencerSnapshot(
        lastTs,
        UninitializedBlockHeight,
        checkpointsAtTimestamp.fmap(_.counter),
        statusAtTimestamp,
        Map.empty,
        None,
        protocolVersion,
        Seq.empty,
        Seq.empty,
      )
    }
  }

  def checkpointsAtTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, CounterCheckpoint]] =
    for {
      sequencerIdO <- lookupMember(sequencerMember).map(_.map(_.memberId))
      query = for {
        safeWatermarkO <- safeWaterMarkDBIO
        safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
        checkpoints <- memberCheckpointsQuery(timestamp, safeWatermark)
        latestSequencerTimestamps <- sequencerIdO match {
          case Some(id) =>
            memberLatestSequencerTimestampQuery(
              timestamp,
              safeWatermark,
              id,
            )
          case _ => DBIO.successful[Map[Member, Option[CantonTimestamp]]](Map())
        }
      } yield {
        checkpoints.map { case (member, checkpoint) =>
          (
            member,
            // TODO(i20011): make sure lastTopologyClientEventTimestamp is consistent
            checkpoint.copy(latestTopologyClientTimestamp =
              latestSequencerTimestamps
                .get(member)
                .flatten
                .orElse(checkpoint.latestTopologyClientTimestamp)
            ),
          )
        }
      }
      result <- storage
        .queryUnlessShutdown(query, functionFullName)
    } yield result

  private def previousCheckpoints(
      beforeInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): DBIOAction[
    (CantonTimestamp, Map[Member, CounterCheckpoint]),
    NoStream,
    Effect.Read,
  ] = {
    val query = storage.profile match {
      case _: Postgres =>
        sql"""
          select m.member, coalesce(cc.counter, -1) as counter, coalesce(cc.ts, ${CantonTimestamp.MinValue}) as ts, cc.latest_sequencer_event_ts
                        from sequencer_members m
                          left join lateral (
                                  select *
                                  from sequencer_counter_checkpoints
                                  where member = m.id and ts <= $beforeInclusive and ts >= m.registered_ts
                                  order by member, ts desc
                                  limit 1
                              ) cc
                              on true
                        where m.enabled = true and m.registered_ts <= $beforeInclusive
          """
      case _ =>
        sql"""
          select m.member, max(cc.counter) as counter, max(cc.ts) as ts, max(cc.latest_sequencer_event_ts) as latest_sequencer_event_ts
          from
            sequencer_members m left join sequencer_counter_checkpoints cc on m.id = cc.member
          where
            cc.ts <= $beforeInclusive and
            m.registered_ts <= $beforeInclusive and
            m.enabled = true
          group by m.member
          """
    }
    query.as[(Member, CounterCheckpoint)].map { previousCheckpoints =>
      val timestamps = previousCheckpoints.view.map { case (_member, checkpoint) =>
        checkpoint.timestamp
      }.toSet - CantonTimestamp.MinValue // in case the member is new, with no prior checkpoints and events

      if (timestamps.sizeIs > 1) {
        // We added an assumption that for any ts1 we can find a checkpoint at ts0 <= ts1,
        // such that we have all enabled members included in that checkpoint.
        // Then instead of filtering for each member individually, we can just filter for the ts0 >
        // when scanning events and this simple filter should be efficient and recognizable by the query planner.
        // If no such checkpoints are found, we return Left to indicate
        ErrorUtil.invalidState(
          s"Checkpoint for all members are not aligned. Found timestamps: $timestamps"
        )
      } else {
        (timestamps.headOption.getOrElse(CantonTimestamp.MinValue), previousCheckpoints.toMap)
      }
    }
  }

  private def memberCheckpointsQuery(
      beforeInclusive: CantonTimestamp,
      safeWatermark: CantonTimestamp,
  )(implicit traceContext: TraceContext) = {
    // this query returns checkpoints for all registered enabled members at the given timestamp
    // it will produce checkpoints at exactly the `beforeInclusive` timestamp by assuming that the checkpoint's
    // `timestamp` doesn't need to be exact as long as it's a valid lower bound for a given (member, counter).
    // it does this by taking existing events and checkpoints before or at the given timestamp in order to compute
    // the equivalent latest checkpoint for each member at or before this timestamp.
    def query(previousCheckpointTimestamp: CantonTimestamp) = storage.profile match {
      case _: Postgres =>
        sql"""
            -- the max counter for each member will be either the number of events -1 (because the index is 0 based)
            -- or the checkpoint counter + number of events after that checkpoint
            -- the timestamp for a member will be the maximum between the highest event timestamp and the checkpoint timestamp (if it exists)
            with
              enabled_members as (
                select
                  member,
                  id
                from sequencer_members
                where
                  -- consider the given timestamp
                  registered_ts <= $beforeInclusive
                  -- no need to consider disabled members since they can't be served events anymore
                  and enabled = true
              ),
              events_per_member as (
                select
                  unnest(events.recipients) member,
                  events.ts,
                  events.node_index
                from sequencer_events events
                where
                  -- we just want the events between the checkpoint and the requested timestamp
                  -- and within the safe watermark
                  events.ts <= $beforeInclusive and events.ts <= $safeWatermark
                  -- start from closest checkpoint the checkpoint is defined, we only want events past it
                  and events.ts > $previousCheckpointTimestamp
              )
            select
              members.member,
              count(events.ts)
            from
              enabled_members members
            left join events_per_member as events
              on  events.member = members.id
            left join sequencer_watermarks watermarks
              on (events.node_index is not null) and events.node_index = watermarks.node_index
            where
              ((events.ts is null) or (
                -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
              ))
            group by members.member
             """
      case _ =>
        sql"""
            -- the max counter for each member will be either the number of events -1 (because the index is 0 based)
            -- or the checkpoint counter + number of events after that checkpoint
            -- the timestamp for a member will be the maximum between the highest event timestamp and the checkpoint timestamp (if it exists)
            select sequencer_members.member, count(events.ts), $beforeInclusive, null  -- null is only used to deserialize the result into `CounterCheckpoint`
            from sequencer_members
            left join (
                -- if the member has checkpoints, let's find the one latest one that's still before or at the given timestamp.
                -- using checkpoints is essential for cases where the db has been pruned
                select member, max(counter) as counter, max(ts) as ts, max(latest_sequencer_event_ts) as latest_sequencer_event_ts
                from sequencer_counter_checkpoints
                where ts <= $beforeInclusive
                group by member
            ) as checkpoints on checkpoints.member = sequencer_members.id
            left join sequencer_events as events
              on ((#$memberContainsBefore sequencer_members.id #$memberContainsAfter)
                      -- we just want the events between the checkpoint and the requested timestamp
                      -- and within the safe watermark
                      and events.ts <= $beforeInclusive and events.ts <= $safeWatermark
                      -- if the checkpoint is defined, we only want events past it
                      and ((checkpoints.ts is null) or (checkpoints.ts < events.ts))
                      -- start from member's registration date
                      and events.ts >= sequencer_members.registered_ts)
            left join sequencer_watermarks watermarks
              on (events.node_index is not null) and events.node_index = watermarks.node_index
            where (
                -- no need to consider disabled members since they can't be served events anymore
                sequencer_members.enabled = true
                -- consider the given timestamp
                and sequencer_members.registered_ts <= $beforeInclusive
                and ((events.ts is null) or (
                    -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                     watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                    ))
              )
            group by (sequencer_members.member, checkpoints.counter, checkpoints.ts, checkpoints.latest_sequencer_event_ts)
            """
    }

    for {
      (previousCheckpointTimestamp, previousCheckpoints) <- previousCheckpoints(beforeInclusive)
      countedEventsSinceCheckpoint <- query(previousCheckpointTimestamp)
        .as[(Member, Long)]
        .map(_.toMap)
    } yield {
      val initialCheckpoint =
        CounterCheckpoint(SequencerCounter(-1), CantonTimestamp.MinValue, None)
      val allMembers = countedEventsSinceCheckpoint.keySet ++ previousCheckpoints.keySet
      // We count the events since the previous checkpoint and add to it to produce a new one
      allMembers.map { member =>
        val addToCounter = countedEventsSinceCheckpoint.getOrElse(member, 0L)
        val checkpoint = previousCheckpoints.getOrElse(member, initialCheckpoint)
        (
          member,
          checkpoint.copy(counter = checkpoint.counter + addToCounter, timestamp = beforeInclusive),
        )
      }.toMap
    }
  }

  private def memberLatestSequencerTimestampQuery(
      beforeInclusive: CantonTimestamp,
      safeWatermark: CantonTimestamp,
      sequencerId: SequencerMemberId,
  )(implicit traceContext: TraceContext) = {
    // in order to compute the latest sequencer event for each member at a timestamp, we find the latest event ts
    // for an event addressed both to the sequencer and that member
    def query(previousCheckpointTimestamp: CantonTimestamp) = storage.profile match {
      case _: Postgres =>
        sql"""
            -- for each member we scan the sequencer_events table
            -- bounded above by the requested `timestamp`, watermark, registration date;
            -- bounded below by an existing sequencer counter (or by beginning of time), by member registration date;
            -- this is crucial to avoid scanning the whole table and using the index on `ts` field
            select sequencer_members.member, max(events.ts)
            from sequencer_members
            left join sequencer_events as events
              on ((sequencer_members.id = any(events.recipients)) -- member is in recipients
                    -- this sequencer itself is in recipients
                    and $sequencerId = any(events.recipients)
                    -- we just want the events between the checkpoint and the requested timestamp
                    -- and within the safe watermark
                    and events.ts <= $beforeInclusive and events.ts <= $safeWatermark
                    -- start from closest checkpoint, we only want events past it
                    and events.ts > $previousCheckpointTimestamp
                    -- start from member's registration date
                    and events.ts >= sequencer_members.registered_ts)
            left join sequencer_watermarks watermarks
              on (events.node_index is not null) and events.node_index = watermarks.node_index
            where (
                -- no need to consider disabled members since they can't be served events anymore
                sequencer_members.enabled = true
                -- consider the given timestamp
                and sequencer_members.registered_ts <= $beforeInclusive
                and ((events.ts is null) or (
                    -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                     watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                    ))
              )
            group by sequencer_members.member
             """
      case _ =>
        sql"""
            select sequencer_members.member, max(events.ts)
            from sequencer_members
            left join sequencer_events as events
              on ((#$memberContainsBefore sequencer_members.id #$memberContainsAfter)
                      and (#$memberContainsBefore $sequencerId #$memberContainsAfter)
                      and events.ts <= $beforeInclusive and events.ts <= $safeWatermark
                      -- start from member's registration date
                      and events.ts >= sequencer_members.registered_ts)
            left join sequencer_watermarks watermarks
              on (events.node_index is not null) and events.node_index = watermarks.node_index
            where (
                -- no need to consider disabled members since they can't be served events anymore
                sequencer_members.enabled = true
                -- consider the given timestamp
                and sequencer_members.registered_ts <= $beforeInclusive
                and events.ts is not null
                -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                and  (watermarks.watermark_ts is not null and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts))
              )
            group by (sequencer_members.member, events.ts)
            """
    }

    for {
      (previousCheckpointTimestamp, previousCheckpoints) <- previousCheckpoints(beforeInclusive)
      latestSequencerTimestampsSincePreviousCheckpoint <- query(previousCheckpointTimestamp)
        .as[(Member, Option[CantonTimestamp])]
        .map(_.toMap)
    } yield {
      val allMembers =
        latestSequencerTimestampsSincePreviousCheckpoint.keySet ++ previousCheckpoints.keySet
      // We pick the timestamp either from previous checkpoint or from the latest event,
      // can be `None` as well if neither are present or if set to `None` in the checkpoint
      allMembers.map { member =>
        val checkpointLatestSequencerTimestamp =
          previousCheckpoints.get(member).flatMap(_.latestTopologyClientTimestamp)
        val latestSequencerTimestamp =
          latestSequencerTimestampsSincePreviousCheckpoint.get(member).flatten
        (member, latestSequencerTimestamp max checkpointLatestSequencerTimestamp)
      }.toMap
    }
  }

  override def deleteEventsPastWatermark(
      instanceIndex: Int
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] =
    for {
      watermarkO <- storage.queryUnlessShutdown(
        sql"select watermark_ts from sequencer_watermarks where node_index = $instanceIndex"
          .as[CantonTimestamp]
          .headOption,
        functionFullName,
      )
      watermark = watermarkO.getOrElse(CantonTimestamp.MinValue)
      // TODO(#18401): Also cleanup payloads (beyond the payload to event margin)
      eventsRemoved <- storage.updateUnlessShutdown(
        sqlu"""
            delete from sequencer_events
            where node_index = $instanceIndex
                and ts > $watermark
           """,
        functionFullName,
      )
    } yield {
      logger.debug(
        s"Removed at least $eventsRemoved that were past the last watermark ($watermarkO) for this sequencer"
      )
      watermarkO
    }

  override def saveCounterCheckpoint(
      memberId: SequencerMemberId,
      checkpoint: CounterCheckpoint,
  )(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SaveCounterCheckpointError, Unit] =
    EitherT.right(
      saveCounterCheckpoints(Seq(memberId -> checkpoint))(traceContext, externalCloseContext)
    )

  override def saveCounterCheckpoints(
      checkpoints: Seq[(SequencerMemberId, CounterCheckpoint)]
  )(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    val insertAllCheckpoints =
      profile match {
        case _: Postgres =>
          val insertQuery =
            """insert into sequencer_counter_checkpoints (member, counter, ts, latest_sequencer_event_ts)
             |values (?, ?, ?, ?)
             |on conflict (member, counter, ts)
             |do update set latest_sequencer_event_ts = ?
             |where excluded.latest_sequencer_event_ts > sequencer_counter_checkpoints.latest_sequencer_event_ts
             |""".stripMargin

          DbStorage
            .bulkOperation(insertQuery, checkpoints, storage.profile) { pp => memberIdCheckpoint =>
              val (memberId, checkpoint) = memberIdCheckpoint
              pp >> memberId
              pp >> checkpoint.counter
              pp >> checkpoint.timestamp
              pp >> checkpoint.latestTopologyClientTimestamp
              pp >> checkpoint.latestTopologyClientTimestamp
            }
            .transactionally

        case _: H2 =>
          val insertQuery =
            """merge into sequencer_counter_checkpoints using dual
              |on member = ? and counter = ? and ts = ?
              |  when not matched then
              |    insert (member, counter, ts, latest_sequencer_event_ts)
              |    values (?, ?, ?, ?)
              |  when matched and latest_sequencer_event_ts < ? then
              |    update set latest_sequencer_event_ts = ?
              |""".stripMargin

          DbStorage
            .bulkOperation(insertQuery, checkpoints, storage.profile) { pp => memberIdCheckpoint =>
              val (memberId, checkpoint) = memberIdCheckpoint
              pp >> memberId
              pp >> checkpoint.counter
              pp >> checkpoint.timestamp
              pp >> memberId
              pp >> checkpoint.counter
              pp >> checkpoint.timestamp
              pp >> checkpoint.latestTopologyClientTimestamp
              pp >> checkpoint.latestTopologyClientTimestamp
              pp >> checkpoint.latestTopologyClientTimestamp
            }
            .transactionally
      }

    CloseContext.withCombinedContext(closeContext, externalCloseContext, timeouts, logger)(
      combinedCloseContext =>
        storage
          .queryAndUpdateUnlessShutdown(insertAllCheckpoints, functionFullName)(
            traceContext,
            combinedCloseContext,
          )
          .map { updateCounts =>
            checkpoints.foreach { case (memberId, checkpoint) =>
              logger.debug(
                s"Saved $checkpoint for member $memberId in the database"
              )
            }
            logger.debug(s"Updated ${updateCounts.sum} counter checkpoints in the database")
            ()
          }
    )
  }

  override def fetchClosestCheckpointBefore(memberId: SequencerMemberId, counter: SequencerCounter)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CounterCheckpoint]] = {
    val checkpointQuery = for {
      // This query has been modified to use the safe watermark, due to a possibility that crash recovery resets the watermark,
      // thus we prevent members from reading data after the watermark. This matters only for the db sequencer.
      safeWatermarkO <- safeWaterMarkDBIO
      safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
      checkpoint <- sql"""
             select counter, ts, latest_sequencer_event_ts
             from sequencer_counter_checkpoints
             where member = $memberId
               and counter < $counter
               and ts <= $safeWatermark
             order by counter desc, ts desc
              #${storage.limit(1)}
             """.as[CounterCheckpoint].headOption
    } yield checkpoint
    storage.queryUnlessShutdown(checkpointQuery, functionFullName)
  }

  override def fetchLatestCheckpoint()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val checkpointQuery = for {
      safeWatermarkO <- safeWaterMarkDBIO
      safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
      checkpoint <- sql"""
        select ts
        from sequencer_counter_checkpoints
        where ts <= $safeWatermark and ts > ${CantonTimestamp.Epoch}
        order by ts desc
        #${storage.limit(1)}"""
        .as[CantonTimestamp]
        .headOption
      checkpointOrMinEvent <- checkpoint match {
        case None =>
          sql"""select ts from sequencer_events
                where ts <= $safeWatermark and ts > ${CantonTimestamp.Epoch}
                order by ts asc
                #${storage.limit(1)}"""
            .as[CantonTimestamp]
            .headOption
        case ts @ Some(_) =>
          DBIO.successful(ts)
      }

    } yield checkpointOrMinEvent

    storage.queryUnlessShutdown(checkpointQuery, functionFullName)
  }

  override def fetchEarliestCheckpointForMember(memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CounterCheckpoint]] = {
    val checkpointQuery = for {
      // This query has been modified to use the safe watermark, due to a possibility that crash recovery resets the watermark,
      // thus we prevent members from reading data after the watermark. This matters only for the db sequencer.
      safeWatermarkO <- safeWaterMarkDBIO
      safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)
      checkpoint <- sql"""
             select counter, ts, latest_sequencer_event_ts
             from sequencer_counter_checkpoints
             where member = $memberId
               and ts <= $safeWatermark
             order by counter desc
              #${storage.limit(1)}
             """.as[CounterCheckpoint].headOption
    } yield checkpoint
    storage.queryUnlessShutdown(checkpointQuery, functionFullName)

  }

  override def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.updateUnlessShutdown_(
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
      .queryUnlessShutdown(
        sql"""
                  select member, ts
                  from sequencer_acknowledgements
           """.as[(SequencerMemberId, CantonTimestamp)],
        functionFullName,
      )
      .map(_.map { case (memberId, timestamp) => memberId -> timestamp })
      .map(_.toMap)

  private def fetchLowerBoundDBIO(): ReadOnly[Option[CantonTimestamp]] =
    sql"select ts from sequencer_lower_bound".as[CantonTimestamp].headOption

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    storage.querySingleUnlessShutdown(fetchLowerBoundDBIO(), "fetchLowerBound").value

  override def saveLowerBound(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SaveLowerBoundError, Unit] =
    EitherT(
      storage.queryAndUpdateUnlessShutdown(
        (for {
          existingTsO <- dbEitherT(fetchLowerBoundDBIO())
          _ <- EitherT.fromEither[DBIO](
            existingTsO
              .filter(_ > ts)
              .map(SaveLowerBoundError.BoundLowerThanExisting(_, ts))
              .toLeft(())
          )
          _ <- dbEitherT[SaveLowerBoundError](
            existingTsO.fold(sqlu"insert into sequencer_lower_bound (ts) values ($ts)")(_ =>
              sqlu"update sequencer_lower_bound set ts = $ts"
            )
          )
        } yield ()).value.transactionally
          .withTransactionIsolation(TransactionIsolation.Serializable),
        "saveLowerBound",
      )
    )

  override protected[store] def pruneEvents(
      beforeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.updateUnlessShutdown(
      sqlu"delete from sequencer_events where ts < $beforeExclusive",
      functionFullName,
    )

  override protected[store] def prunePayloads(
      beforeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.updateUnlessShutdown(
      sqlu"delete from sequencer_payloads where id < $beforeExclusive",
      functionFullName,
    )

  override protected[store] def pruneCheckpoints(
      beforeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    for {
      checkpointsRemoved <- storage.updateUnlessShutdown(
        sqlu"""
          delete from sequencer_counter_checkpoints where ts < $beforeExclusive
          """,
        functionFullName,
      )
    } yield checkpointsRemoved

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = storage
    .querySingleUnlessShutdown(
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
      members <- storage.queryUnlessShutdown(
        sql"""
      select member, id, registered_ts, enabled from sequencer_members where registered_ts <= $now"""
          .as[(Member, SequencerMemberId, CantonTimestamp, Boolean)],
        functionFullName,
      )
      acknowledgements <- latestAcknowledgements()
    } yield {
      SequencerPruningStatus(
        lowerBound = lowerBoundO.getOrElse(CantonTimestamp.Epoch),
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
      rowsUpdated <- storage.updateUnlessShutdown(
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
      storage.queryUnlessShutdown(statement.as[Long].head, functionFullName)

    for {
      events <- count(sql"select count(*) from sequencer_events")
      payloads <- count(sql"select count(*) from sequencer_payloads")
      counterCheckpoints <- count(sql"select count(*) from sequencer_counter_checkpoints")
    } yield SequencerStoreRecordCounts(events, payloads, counterCheckpoints)
  }

  /** Count stored events for this node. Used exclusively by tests. */
  @VisibleForTesting
  private[store] def countEventsForNode(
      instanceIndex: Int
  )(implicit traceContext: TraceContext): Future[Int] =
    storage.query(
      sql"select count(ts) from sequencer_events where node_index = $instanceIndex".as[Int].head,
      functionFullName,
    )

  override def disableMemberInternal(member: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    // we assume here that the member is already registered in order to have looked up the memberId
    storage.updateUnlessShutdown_(
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
            storage.queryUnlessShutdown(
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

  override def recordCounterCheckpointsAtTimestamp(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Recording counter checkpoints for all members at timestamp $timestamp")
    val now = CantonTimestamp.now()
    for {
      checkpoints <- checkpointsAtTimestamp(timestamp)
      checkpointsByMemberId <- checkpoints.toList
        .parTraverseFilter { case (member, checkpoint) =>
          lookupMember(member).map(_.map(_.memberId -> checkpoint))
        }
      _ <- saveCounterCheckpoints(checkpointsByMemberId)(traceContext, externalCloseContext)
    } yield {
      logger.debug(
        s"Recorded counter checkpoints for all members at timestamp $timestamp in ${CantonTimestamp.now() - now}"
      )
    }
  }
}
