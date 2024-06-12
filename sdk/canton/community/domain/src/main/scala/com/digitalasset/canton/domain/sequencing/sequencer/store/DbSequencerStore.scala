// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.catsinstances.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.UninitializedBlockHeight
import com.digitalasset.canton.domain.sequencing.sequencer.{
  CommitMode,
  SequencerMemberStatus,
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.*
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Oracle, Postgres}
import com.digitalasset.canton.resource.DbStorage.*
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import com.zaxxer.hikari.pool.HikariProxyConnection
import oracle.jdbc.{OracleArray, OracleConnection}
import org.h2.api.ErrorCode as H2ErrorCode
import org.postgresql.util.PSQLState
import slick.jdbc.*

import java.sql.{Connection, JDBCType, SQLException, SQLNonTransientException}
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Database backed sequencer store.
  * Supports many concurrent instances reading and writing to the same backing database.
  */
class DbSequencerStore(
    storage: DbStorage,
    protocolVersion: ProtocolVersion,
    maxInClauseSize: PositiveNumeric[Int],
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
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
      : SetParameter[Option[NonEmpty[SortedSet[SequencerMemberId]]]] = (v, pp) => {
    storage.profile match {
      case _: Oracle =>
        val OracleIntegerArray = "INTEGER_ARRAY"

        val maybeArray: Option[Array[Int]] = v.map(_.toArray.map(_.unwrap))

        // make sure we do the right thing whether we are using a connection pooled connection or not
        val jdbcArray = maybeArray.map {
          pp.ps.getConnection match {
            case hikari: HikariProxyConnection =>
              hikari.unwrap(classOf[OracleConnection]).createARRAY(OracleIntegerArray, _)
            case oracle: OracleConnection =>
              oracle.createARRAY(OracleIntegerArray, _)
            case c: Connection =>
              sys.error(
                s"Unsupported connection type for creating Oracle integer array: ${c.getClass.getSimpleName}"
              )
          }
        }

        // we need to bypass the slick wrapper because we need to call the setNull method below tailored for
        // user defined types since we are using a custom oracle array
        def setOracleArrayOption(value: Option[AnyRef], sqlType: Int): Unit = {
          val npos = pp.pos + 1
          value match {
            case Some(v) => pp.ps.setObject(npos, v, sqlType)
            case None => pp.ps.setNull(npos, sqlType, OracleIntegerArray)
          }
          pp.pos = npos
        }
        setOracleArrayOption(jdbcArray, JDBCType.ARRAY.getVendorTypeNumber)

      case _ =>
        val jdbcArray = v
          .map(_.toArray.map(id => Int.box(id.unwrap): AnyRef))
          .map(pp.ps.getConnection.createArrayOf("integer", _))

        pp.setObjectOption(jdbcArray, JDBCType.ARRAY.getVendorTypeNumber)

    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private implicit val getRecipientsArrayOResults
      : GetResult[Option[NonEmpty[SortedSet[SequencerMemberId]]]] = {

    def toInt(a: AnyRef) = a match {
      case s: String => s.toInt
      case null =>
        throw new SQLNonTransientException(s"Cannot convert object array element null to Int")
      case invalid =>
        throw new SQLNonTransientException(
          s"Cannot convert object array element (of type ${invalid.getClass.getName}) to Int"
        )
    }

    storage.profile match {
      case _: Oracle =>
        GetResult(r => Option(r.rs.getArray(r.skip.currentPos)))
          .andThen(_.map(_.asInstanceOf[OracleArray].getIntArray))
          .andThen(_.map(_.map(SequencerMemberId(_))))
          .andThen(_.map(arr => NonEmptyUtil.fromUnsafe(SortedSet(arr.toSeq*))))
      case _: H2 =>
        GetResult(r => Option(r.rs.getArray(r.skip.currentPos)))
          .andThen(_.map(_.getArray.asInstanceOf[Array[AnyRef]].map(toInt)))
          .andThen(_.map(_.map(SequencerMemberId(_))))
          .andThen(_.map(arr => NonEmptyUtil.fromUnsafe(SortedSet(arr.toSeq*))))
      case _: Postgres =>
        GetResult(r => Option(r.rs.getArray(r.skip.currentPos)))
          .andThen(_.map(_.getArray.asInstanceOf[Array[AnyRef]].map(Int.unbox)))
          .andThen(_.map(_.map(SequencerMemberId(_))))
          .andThen(_.map(arr => NonEmptyUtil.fromUnsafe(SortedSet(arr.toSeq*))))
    }
  }

  private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(protocolVersion)

  /** Single char that is persisted with the event to indicate the type of event */
  sealed abstract class EventTypeDiscriminator(val value: Char)

  object EventTypeDiscriminator {

    case object Receipt extends EventTypeDiscriminator('R')
    case object Deliver extends EventTypeDiscriminator('D')

    case object Error extends EventTypeDiscriminator('E')

    private val all = Seq[EventTypeDiscriminator](Deliver, Error)

    def fromChar(value: Char): Either[String, EventTypeDiscriminator] =
      all.find(_.value == value).toRight(s"Event type discriminator for value [$value] not found")
  }

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
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
          )
        case DeliverErrorStoreEvent(sender, messageId, errorO, traceContext) =>
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
          )
        case ReceiptStoreEvent(sender, messageId, topologyTimestampO, traceContext) =>
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

  private implicit val getDeliverStoreEventRowResult: GetResult[Sequenced[Payload]] = {
    val timestampGetter = implicitly[GetResult[CantonTimestamp]]
    val timestampOGetter = implicitly[GetResult[Option[CantonTimestamp]]]
    val discriminatorGetter = implicitly[GetResult[EventTypeDiscriminator]]
    val messageIdGetter = implicitly[GetResult[Option[MessageId]]]
    val memberIdGetter = implicitly[GetResult[Option[SequencerMemberId]]]
    val memberIdNesGetter = implicitly[GetResult[Option[NonEmpty[SortedSet[SequencerMemberId]]]]]
    val payloadGetter = implicitly[GetResult[Option[Payload]]]
    val traceContextGetter = implicitly[GetResult[SerializableTraceContext]]
    val errorOGetter = implicitly[GetResult[Option[ByteString]]]

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
      )

      row.asStoreEvent
        .fold(
          msg => throw new DbDeserializationException(s"Failed to deserialize event row: $msg"),
          identity,
        )
    }
  }

  private val profile = storage.profile

  override def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SequencerMemberId] =
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
          case _: Oracle =>
            sqlu"""insert /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( sequencer_members ( member ) ) */
                   into sequencer_members (member, registered_ts)
                   values ($member, $timestamp)
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
  ): Future[Option[RegisteredMember]] =
    storage.query(
      sql"""select id, registered_ts from sequencer_members where member = $member"""
        .as[(SequencerMemberId, CantonTimestamp)]
        .headOption
        .map(_.map(RegisteredMember.tupled)),
      functionFullName,
    )

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
  override def savePayloads(payloads: NonEmpty[Seq[Payload]], instanceDiscriminator: UUID)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SavePayloadsError, Unit] = {

    // insert the provided payloads with the associated discriminator to the payload table.
    // we're intentionally using a insert that will fail with a primary key constraint violation if rows exist
    def insert(payloadsToInsert: NonEmpty[Seq[Payload]]): Future[Boolean] = {
      def isConstraintViolation(batchUpdateException: SQLException): Boolean = profile match {
        case Postgres(_) => batchUpdateException.getSQLState == PSQLState.UNIQUE_VIOLATION.getState
        case Oracle(_) =>
          // error code for a unique constraint violation
          // see: https://docs.oracle.com/en/database/oracle/oracle-database/19/errmg/ORA-00000.html#GUID-27437B7F-F0C3-4F1F-9C6E-6780706FB0F6
          batchUpdateException.getMessage.contains("ORA-00001")
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
          case Success(rowCounts) =>
            val allRowsInserted = rowCounts.forall(_ > 0)
            Success(allRowsInserted)

          // if the only exceptions we received are constraint violations then just check and maybe try again
          case Failure(batchUpdateException: java.sql.BatchUpdateException)
              if areAllConstraintViolations(batchUpdateException) =>
            Success(false)

          case Failure(otherThrowable) => Failure(otherThrowable)
        }
    }

    // work out from the provided set of payloads that we attempted to insert which are now stored successfully
    // and which are still missing.
    // will return an error if the payload exists but with a different uniquifier as this suggests another process
    // has inserted a conflicting value.
    def listMissing(): EitherT[Future, SavePayloadsError, Seq[Payload]] = {
      val payloadIds = payloads.map(_.id)
      // the max default config for number of payloads is around 50 and the max number of clauses that oracle supports is around 1000
      // so we're really unlikely to need to this IN clause splitting, but lets support it just in case as Matthias has
      // already done the heavy lifting :)
      val queries = DbStorage
        .toInClauses_("id", payloadIds, maxInClauseSize)
        .map { in =>
          (sql"select id, instance_discriminator from sequencer_payloads where " ++ in)
            .as[(PayloadId, UUID)]
        }

      for {
        inserted <- EitherT.right {
          storage.sequentialQueryAndCombine(queries, functionFullName)
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
          .toEitherT[Future]
      } yield missing
    }

    def go(
        remainingPayloadsToInsert: NonEmpty[Seq[Payload]]
    ): EitherT[Future, SavePayloadsError, Unit] =
      EitherT
        .right(insert(remainingPayloadsToInsert))
        .flatMap { successful =>
          if (!successful) listMissing()
          else EitherT.pure[Future, SavePayloadsError](Seq.empty[Payload])
        }
        .flatMap { missing =>
          // do we have any remaining to insert
          NonEmpty.from(missing).fold(EitherTUtil.unit[SavePayloadsError]) { missing =>
            logger.debug(
              s"Retrying to insert ${missing.size} missing of ${remainingPayloadsToInsert.size} payloads"
            )
            go(missing)
          }
        }

    go(payloads)
  }

  override def saveEvents(instanceIndex: Int, events: NonEmpty[Seq[Sequenced[PayloadId]]])(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val saveSql = storage.profile match {
      case _: H2 | _: Postgres =>
        """insert into sequencer_events (
                                    |  ts, node_index, event_type, message_id, sender, recipients,
                                    |  payload_id, topology_timestamp, trace_context, error
                                    |)
                                    |  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    |  on conflict do nothing""".stripMargin
      case _: Oracle =>
        """merge /*+ INDEX ( sequencer_events ( ts ) ) */
          |into sequencer_events
          |using (select ? ts from dual) input
          |on (sequencer_events.ts = input.ts)
          |when not matched then
          |  insert (ts, node_index, event_type, message_id, sender, recipients, payload_id,
          |  topology_timestamp, trace_context, error)
          |  values (input.ts, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
    }

    storage.queryAndUpdate(
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
      },
      functionFullName,
    )
  }

  override def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveWatermarkError, Unit] = {
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
        case _: Oracle =>
          sqlu"""merge into sequencer_watermarks using dual
                  on (node_index = $instanceIndex)
                  when matched then
                    update set watermark_ts = $ts where sequencer_online = ${true}
                  when not matched then
                    insert (node_index, watermark_ts, sequencer_online) values ($instanceIndex, $ts, ${true})
                """
      }

    for {
      _ <- EitherT.right(storage.update(save, functionFullName))
      updatedWatermarkO <- EitherT.right(fetchWatermark(instanceIndex))
      // we should have just inserted or updated a watermark, so should certainly exist
      updatedWatermark <- EitherT.fromEither[Future](
        updatedWatermarkO.toRight(
          WatermarkUnexpectedlyChanged("The watermark we should have written has been removed")
        )
      )
      // check we're still online
      _ <- EitherTUtil.condUnitET[Future](updatedWatermark.online, WatermarkFlaggedOffline)
      // check the timestamp is what we've just written.
      // if not it implies that another sequencer instance is writing with the same node index, most likely due to a
      // configuration error.
      // note we'd only observe this if the other sequencer watermark update is interleaved with ours
      // which drastically limits the possibility of observing this mis-configuration scenario.
      _ <- EitherTUtil
        .condUnitET[Future](
          updatedWatermark.timestamp == ts,
          WatermarkUnexpectedlyChanged(s"We wrote $ts but read back ${updatedWatermark.timestamp}"),
        )
        .leftWiden[SaveWatermarkError]
    } yield ()
  }

  override def fetchWatermark(
      instanceIndex: Int,
      maxRetries: Int = retry.Forever,
  )(implicit traceContext: TraceContext): Future[Option[Watermark]] =
    storage
      .querySingle(
        {
          val query =
            sql"""select watermark_ts, sequencer_online
                    from sequencer_watermarks
                    where node_index = $instanceIndex"""
          def watermark(row: (CantonTimestamp, Boolean)) = Watermark(row._1, row._2)

          profile match {
            case _: H2 | _: Postgres =>
              query.as[(CantonTimestamp, Boolean)].headOption.map(_.map(watermark))
            case _: Oracle =>
              query
                .as[(CantonTimestamp, Int)]
                .headOption
                .map(_.map { case (ts, onlineN) =>
                  watermark((ts, onlineN != 0))
                })
          }
        },
        functionFullName,
        maxRetries,
      )
      .value

  override def goOffline(instanceIndex: Int)(implicit traceContext: TraceContext): Future[Unit] =
    storage.update_(
      {
        profile match {
          case _: H2 | _: Postgres =>
            sqlu"update sequencer_watermarks set sequencer_online = false where node_index = $instanceIndex"
          case _: Oracle =>
            sqlu"update sequencer_watermarks set sequencer_online = 0 where node_index = $instanceIndex"
        }
      },
      functionFullName,
    )

  override def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[CantonTimestamp] =
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
            case _: H2 | _: Oracle =>
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

  override def fetchOnlineInstances(implicit traceContext: TraceContext): Future[SortedSet[Int]] =
    storage
      .query(
        sql"select node_index from sequencer_watermarks where sequencer_online = ${true}".as[Int],
        functionFullName,
      )
      .map(items => SortedSet(items*))

  override def readEvents(
      memberId: SequencerMemberId,
      fromTimestampO: Option[CantonTimestamp],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): Future[ReadEvents] = {

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
          events.recipients, payloads.id, payloads.content, events.topology_timestamp,
          events.trace_context, events.error
        from sequencer_events events
        left join sequencer_payloads payloads
          on events.payload_id = payloads.id
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

        case _: Oracle =>
          sql"""
          select events.ts, events.node_index, events.event_type, events.message_id, events.sender,
            events.recipients, payloads.id, payloads.content, events.topology_timestamp,
            events.trace_context, events.error
          from sequencer_events events
          left join sequencer_payloads payloads
            on events.payload_id = payloads.id
          inner join sequencer_watermarks watermarks
            on events.node_index = watermarks.node_index
          where
            ((events.recipients is null) or $memberId IN (SELECT * FROM TABLE(events.recipients)))
            and (
              -- inclusive timestamp bound that defaults to MinValue if unset
              events.ts >= $inclusiveFromTimestamp
                -- only consider events within the safe watermark
                and events.ts <= $safeWatermark
                -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                and (watermarks.sequencer_online <> 0 or events.ts <= watermarks.watermark_ts)
              )
          order by events.ts asc
          fetch next $limit rows only""".stripMargin
      }

      query.as[Sequenced[Payload]]
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

  private def safeWaterMarkDBIO: DBIOAction[Option[CantonTimestamp], NoStream, Effect.Read] = {
    val query = profile match {
      case _: H2 | _: Postgres =>
        sql"select min(watermark_ts) from sequencer_watermarks where sequencer_online = true"
      case _: Oracle =>
        sql"select min(watermark_ts) from sequencer_watermarks where sequencer_online <> 0"
    }
    // `min` may return null that is wrapped into None
    query.as[Option[CantonTimestamp]].headOption.map(_.flatten)
  }

  override def readStateAtTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[SequencerSnapshot] = {
    def h2PostgresQueryMemberCheckpoints(
        memberContainsBefore: String,
        memberContainsAfter: String,
        safeWatermark: CantonTimestamp,
    ) = {
      // this query returns checkpoints for all registered enabled members for the given timestamp.
      // it does this by taking existing events and checkpoints before or at the given timestamp in order to compute
      // the equivalent latest checkpoint for each member at or before this timestamp.
      sql"""
        -- the max counter for each member will be either the number of events -1 (because the index is 0 based)
        -- or the checkpoint counter + number of events after that checkpoint
        -- the timestamp for a member will be the maximum between the highest event timestamp and the checkpoint timestamp (if it exists)
        select sequencer_members.member, coalesce(checkpoints.counter, - 1) + count(events.ts), coalesce(max(events.ts), checkpoints.ts), checkpoints.latest_sequencer_event_ts
        from sequencer_members
        left join (
            -- if the member has checkpoints, let's find the one latest one that's still before or at the given timestamp.
            -- using checkpoints is essential for cases where the db has been pruned
            select member, max(counter) as counter, max(ts) as ts, max(latest_sequencer_event_ts) as latest_sequencer_event_ts
            from sequencer_counter_checkpoints
            where ts <= $timestamp
            group by member
        ) as checkpoints on checkpoints.member = sequencer_members.id
        left join sequencer_events as events
          on ((#$memberContainsBefore sequencer_members.id #$memberContainsAfter)
                  -- if the checkpoint is defined, we only want events past it
                  and ((checkpoints.ts is null) or (checkpoints.ts < events.ts)))
        left join sequencer_watermarks watermarks
          on (events.node_index is not null) and events.node_index = watermarks.node_index
        where (
            -- no need to consider disabled members since they can't be served events anymore
            sequencer_members.enabled = true
            -- consider the given timestamp
            and sequencer_members.registered_ts <= $timestamp
            and ((events.ts is null) or (
                events.ts <= $timestamp
                -- only consider events within the safe watermark
                 and events.ts <= $safeWatermark
                -- if the sequencer that produced the event is offline, only consider up until its offline watermark
                 and watermarks.watermark_ts is not null
                 and (watermarks.sequencer_online = true or events.ts <= watermarks.watermark_ts)
                ))
          )
        group by (sequencer_members.member, checkpoints.counter, checkpoints.ts, checkpoints.latest_sequencer_event_ts)
        """
    }

    def queryMemberCheckpoints(safeWatermarkO: Option[CantonTimestamp]) = {
      val safeWatermark = safeWatermarkO.getOrElse(CantonTimestamp.MaxValue)

      val query = profile match {
        case _: Postgres =>
          h2PostgresQueryMemberCheckpoints("", " = any(events.recipients)", safeWatermark)

        case _: H2 =>
          h2PostgresQueryMemberCheckpoints("array_contains(events.recipients, ", ")", safeWatermark)

        case _: Oracle => sys.error("Oracle no longer supported")
      }
      query.as[(Member, CounterCheckpoint)]
    }

    val query = for {
      safeWatermark <- safeWaterMarkDBIO
      counters <- queryMemberCheckpoints(safeWatermark)
    } yield counters

    for {
      checkpointsAtTimestamp <- storage.query(query.transactionally, functionFullName)
      lastTs = checkpointsAtTimestamp
        .map(_._2.timestamp)
        .maxOption
        .getOrElse(CantonTimestamp.MinValue)
      statusAtTimestamp <- status(lastTs)
    } yield {
      val checkpoints = checkpointsAtTimestamp.toMap
      SequencerSnapshot(
        lastTs,
        UninitializedBlockHeight,
        checkpoints.fmap(_.counter),
        statusAtTimestamp,
        Map.empty,
        None,
        protocolVersion,
        Map.empty,
        Seq.empty,
      )
    }
  }

  override def deleteEventsPastWatermark(
      instanceIndex: Int
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      eventsRemoved <- storage.update(
        {
          sqlu"""
            delete from sequencer_events
            where node_index = $instanceIndex
                and ts > coalesce((select watermark_ts from sequencer_watermarks where node_index = $instanceIndex), -1)
           """
        },
        functionFullName,
      )
    } yield logger.debug(
      s"Removed at least $eventsRemoved that were past the last watermark for this sequencer"
    )

  override def saveCounterCheckpoint(
      memberId: SequencerMemberId,
      checkpoint: CounterCheckpoint,
  )(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): EitherT[Future, SaveCounterCheckpointError, Unit] = {

    EitherT {
      val CounterCheckpoint(counter, ts, latestSequencerEventTimestamp) = checkpoint
      CloseContext.withCombinedContext(closeContext, externalCloseContext, timeouts, logger)(
        combinedCloseContext =>
          storage.queryAndUpdate(
            for {
              _ <- profile match {
                case _: Postgres =>
                  sqlu"""insert into sequencer_counter_checkpoints (member, counter, ts, latest_sequencer_event_ts)
             values ($memberId, $counter, $ts, $latestSequencerEventTimestamp)
             on conflict (member, counter) do nothing
             """
                case _: H2 =>
                  sqlu"""merge into sequencer_counter_checkpoints using dual
                    on member = $memberId and counter = $counter
                    when not matched then
                      insert (member, counter, ts, latest_sequencer_event_ts)
                      values ($memberId, $counter, $ts, $latestSequencerEventTimestamp)
                  """
                case _: Oracle =>
                  sqlu""" insert /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( sequencer_counter_checkpoints ( member, counter ) ) */
            into sequencer_counter_checkpoints (member, counter, ts, latest_sequencer_event_ts)
          values ($memberId, $counter, $ts, $latestSequencerEventTimestamp)
          """
              }
              id <- sql"""
            select ts, latest_sequencer_event_ts
              from sequencer_counter_checkpoints
              where member = $memberId and counter = $counter
              """
                .as[(CantonTimestamp, Option[CantonTimestamp])]
                .headOption
            } yield id,
            functionFullName,
          )(traceContext, combinedCloseContext)
      ) map {
        case None =>
          // we should always return a value from the db statement so this is a bug or unexpected behavior
          ErrorUtil.internalError(
            new RuntimeException(
              s"saveCounterCheckpoint did not return a timestamp value as expected"
            )
          )

        case Some((storedTs, storedLatestSequencerEventTimestampO)) =>
          Either.cond(
            storedTs == ts && (storedLatestSequencerEventTimestampO == latestSequencerEventTimestamp || storedLatestSequencerEventTimestampO.isEmpty),
            (),
            SaveCounterCheckpointError.CounterCheckpointInconsistent(
              storedTs,
              storedLatestSequencerEventTimestampO,
            ),
          )
      }
    }
  }

  override def fetchClosestCheckpointBefore(memberId: SequencerMemberId, counter: SequencerCounter)(
      implicit traceContext: TraceContext
  ): Future[Option[CounterCheckpoint]] =
    storage
      .query(
        sql"""
           select counter, ts, latest_sequencer_event_ts
           from sequencer_counter_checkpoints
           where member = $memberId
             and counter < $counter
           order by counter desc
            #${storage.limit(1)}
           """.as[CounterCheckpoint].headOption,
        functionFullName,
      )

  override def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
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
        case _: Oracle =>
          sqlu"""merge into sequencer_acknowledgements using dual
                  on (member = $member)
                  when matched then
                    update set ts = $timestamp where $timestamp > ts
                  when not matched then
                    insert (member, ts) values ($member, $timestamp)
                """
      },
      functionFullName,
    )

  override def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): Future[Map[SequencerMemberId, CantonTimestamp]] =
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

  private def fetchLowerBoundDBIO(): ReadOnly[Option[CantonTimestamp]] =
    sql"select ts from sequencer_lower_bound".as[CantonTimestamp].headOption

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    storage.querySingle(fetchLowerBoundDBIO(), "fetchLowerBound").value

  override def saveLowerBound(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, SaveLowerBoundError, Unit] = {
    EitherT(
      storage.queryAndUpdate(
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
  }

  override protected[store] def adjustPruningTimestampForCounterCheckpoints(
      timestamp: CantonTimestamp,
      disabledMembers: Seq[SequencerMemberId],
  )(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = {

    // query the lowest suitable timestamp for each member.
    // it would probably be better to do the ignore and aggregation in sql
    // however this way we don't have to deal with generating a `not in (..)` for
    // ignoredMemberIds and the returned result set will only have as many rows
    // as registered members.
    storage
      .query(
        sql"""
                    select members.id, coalesce(checkpoints.ts, members.registered_ts) ts
                    from sequencer_members members
                    left join (
                     select member, max(ts) ts
                     from sequencer_counter_checkpoints
                     where ts < $timestamp
                     group by member
                    ) checkpoints
                      on members.id = checkpoints.member
                   """.as[(SequencerMemberId, CantonTimestamp)],
        functionFullName,
      )
      .map(_.collect {
        // exclude ignored members
        case (memberId, ts) if !disabledMembers.contains(memberId) => ts
      })
      // just take the lowest
      .map(_.minimumOption)
  }

  override protected[store] def pruneEvents(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] =
    storage.update(
      sqlu"delete from sequencer_events where ts < $timestamp",
      functionFullName,
    )

  override protected[store] def prunePayloads(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] =
    storage.update(
      sqlu"delete from sequencer_payloads where id < $timestamp",
      functionFullName,
    )

  override protected[store] def pruneCheckpoints(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] =
    for {
      checkpointsRemoved <- storage.update(
        sqlu"""
          delete from sequencer_counter_checkpoints where ts < $timestamp
          """,
        functionFullName,
      )
    } yield checkpointsRemoved

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = storage
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
  )(implicit traceContext: TraceContext): Future[SequencerPruningStatus] = {
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
        lowerBound = lowerBoundO.getOrElse(CantonTimestamp.Epoch),
        now = now,
        members = members.map { case (member, memberId, registeredAt, enabled) =>
          SequencerMemberStatus(
            member,
            registeredAt,
            lastAcknowledged = acknowledgements.get(memberId),
            enabled = enabled,
          )
        },
      )
    }
  }

  override def markLaggingSequencersOffline(
      cutoffTime: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
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
  override protected[store] def countRecords(implicit
      traceContext: TraceContext
  ): Future[SequencerStoreRecordCounts] = {
    def count(statement: canton.SQLActionBuilder): Future[Long] =
      storage.query(statement.as[Long].head, functionFullName)

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

  override def disableMember(member: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    // we assume here that the member is already registered in order to have looked up the memberId
    storage.update_(
      sqlu"update sequencer_members set enabled = ${false} where id = $member",
      functionFullName,
    )

  override def isEnabled(member: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    storage
      .query(
        sql"select enabled from sequencer_members where id = $member".as[Boolean].headOption,
        s"$functionFullName:isMemberEnabled",
      )
      .map(
        _.getOrElse(false)
      ) // if the member isn't registered this should be picked up elsewhere

  override def validateCommitMode(
      configuredCommitMode: CommitMode
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    val stringReader = GetResult.GetString
    storage.profile match {
      case H2(_) =>
        // we don't worry about replicas or commit modes in h2
        EitherTUtil.unit
      case Oracle(_) =>
        // TODO(#6942): unknown how to query the current commit mode for oracle
        EitherTUtil.unit
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
            .toEitherT[Future]
          _ <- EitherTUtil.condUnitET[Future](
            configuredCommitMode.postgresSettings.toList.contains(setting),
            s"Postgres 'synchronous_commit' setting is '$setting' but expecting one of: ${configuredCommitMode.postgresSettings.toList
                .mkString(",")}",
          )
        } yield ()
    }
  }
}
