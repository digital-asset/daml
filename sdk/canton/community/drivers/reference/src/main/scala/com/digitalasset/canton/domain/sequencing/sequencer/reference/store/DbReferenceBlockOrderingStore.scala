// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference.store

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockOrderer
import com.digitalasset.canton.domain.block.BlockOrderingSequencer.BatchTag
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.DbReferenceBlockOrderingStore.deserializeBytes
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore.TimestampedBlock
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.v1 as proto
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Oracle, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX
import com.digitalasset.canton.tracing.{TraceContext, Traced, W3CTraceContext}
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.RetryUtil
import com.digitalasset.canton.util.retry.RetryUtil.{
  ErrorKind,
  ExceptionRetryable,
  FatalErrorKind,
  TransientErrorKind,
}
import org.postgresql.util.PSQLException
import slick.jdbc.{GetResult, SetParameter, TransactionIsolation}

import java.util.UUID
import scala.annotation.unused
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class DbReferenceBlockOrderingStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends ReferenceBlockOrderingStore
    with DbStore {
  import storage.api.*

  private val profile = storage.profile

  private implicit val requestGetResult: GetResult[Traced[BlockOrderer.OrderedRequest]] =
    GetResult(s =>
      deserializeBytes(s.nextBytes()) match {
        case Right(ev) => ev
        case Left(error) =>
          throw new DbDeserializationException(s"Could not deserialize block request: $error")
      }
    )

  @unused // used implicitly by database
  private implicit val requestSetParam: SetParameter[Traced[BlockOrderer.OrderedRequest]] =
    (v, pp) => {
      val traceContext = v.traceContext
      val traceparent = traceContext.asW3CTraceContext.map(_.parent).getOrElse("")
      val protoRequest: proto.TracedBlockOrderingRequest =
        proto.TracedBlockOrderingRequest(
          traceparent,
          v.value.tag,
          v.value.body,
          v.value.microsecondsSinceEpoch,
        )
      pp.setBytes(protoRequest.toByteArray)
    }

  def insertRequestWithHeight(blockHeight: Long, request: BlockOrderer.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val uuid = LengthLimitedString.getUuid // uuid is only used so that inserts are idempotent
    val tracedRequest = Traced(request)

    // Logging the UUID to be able to correlate the block on the read side.
    logger.debug(s"Storing an ordered request with UUID: $uuid")

    storage.update_(
      profile match {
        case _: Postgres =>
          sqlu"""insert into blocks (id, request, uuid)
                 values ($blockHeight , $tracedRequest, $uuid)
                 on conflict (id) do nothing
                 """
        case _: H2 =>
          sqlu"""merge into blocks (id, request, uuid)
                    values ($blockHeight, $tracedRequest, $uuid)
                      """
        case _: Oracle =>
          sys.error("reference sequencer does not support oracle database")
      },
      s"insert block with height $blockHeight",
    )
  }

  override def insertRequest(request: BlockOrderer.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val uuid =
      LengthLimitedString.getUuid // uuid is only used so that inserts are idempotent and for logging
    val tracedRequest = Traced(request)

    // Logging the UUID to be able to correlate the block on the read side.
    logger.debug(s"Storing an ordered request with UUID: $uuid")

    def insertBlock() =
      storage.update_(
        (profile match {
          case _: Postgres =>
            sqlu"""insert into blocks (id, request, uuid)
                     values ((select coalesce(max(id) + 1, 0) from blocks) , $tracedRequest, $uuid)
                     on conflict (uuid) do nothing
                     """
          case _: H2 =>
            sqlu"""merge into blocks using (SELECT coalesce(max(id) + 1, 0) AS new_id FROM blocks) vals
                            on uuid = $uuid
                            when not matched then
                              insert (id, request, uuid)
                              values (vals.new_id, $tracedRequest, $uuid)
                          """
          case _: Oracle =>
            sys.error("reference sequencer does not support oracle database")
        }).transactionally
          // serializable isolation level will avoid too much retrying due to key collisions
          .withTransactionIsolation(TransactionIsolation.Serializable),
        "insert block",
      )

    implicit val success: retry.Success[Unit] = retry.Success.always

    retry
      .Backoff(
        logger,
        closeContext.context,
        maxRetries = retry.Forever,
        initialDelay = 50.milliseconds,
        maxDelay = timeouts.storageMaxRetryInterval.unwrap,
        operationName = "insert block",
      )
      .apply(
        insertBlock(),
        new ExceptionRetryable {
          override def retryOK(
              outcome: Try[?],
              logger: TracedLogger,
              lastErrorKind: Option[ErrorKind],
          )(implicit
              tc: TraceContext
          ): RetryUtil.ErrorKind = {
            outcome match {
              case util.Success(_) => RetryUtil.NoErrorKind
              case util.Failure(exception) =>
                exception match {
                  // We want to retry on duplicate key violation because multiple sequencers may try to assign
                  // the same block id (height) to a new block, in which case they should retry with a different
                  // value.
                  // This retry should not be hit very often due to us using serializable isolation level on this operation.
                  // Error codes documented here: https://www.postgresql.org/docs/9.6/errcodes-appendix.html
                  case exception: PSQLException if exception.getSQLState == "23505" =>
                    logger.debug("Retrying block insert because of key collision")(tc)
                    TransientErrorKind
                  case _ => FatalErrorKind
                }
            }
          }
        },
      )
  }

  override def maxBlockHeight()(implicit
      traceContext: TraceContext
  ): Future[Option[Long]] =
    storage.query(sql"""select max(id) from blocks""".as[Option[Long]].head, "max block height")

  override def queryBlocks(initialHeight: Long)(implicit
      traceContext: TraceContext
  ): Future[Seq[TimestampedBlock]] =
    storage
      .query(
        sql"""select id, request, uuid from blocks where id >= $initialHeight order by id"""
          .as[(Long, Traced[BlockOrderer.OrderedRequest], String)]
          .transactionally
          // Serializable isolation level to prevent skipping over blocks producing gaps
          .withTransactionIsolation(TransactionIsolation.Serializable),
        "query blocks",
      )
      .map { requests =>
        // because we've added the ability to specify the block index on insertions, we also need to make sure to
        // never return a sequence of blocks with a gap
        val requestsUntilFirstGap = requests
          .foldLeft(
            (initialHeight - 1, List[(Long, Traced[BlockOrderer.OrderedRequest], UUID)]())
          ) { case ((previousHeight, list), (currentHeight, element, uuid)) =>
            if (currentHeight == previousHeight + 1)
              (currentHeight, list :+ (currentHeight, element, UUID.fromString(uuid)))
            else (previousHeight, list)
          }
          ._2
        requestsUntilFirstGap.map { case (height, tracedRequest, uuid) =>
          val (orderedRequests, lastTopologyTimestamp) =
            if (tracedRequest.value.tag == BatchTag) {
              val tracedBatchedBlockOrderingRequests = proto.TracedBatchedBlockOrderingRequests
                .parseFrom(tracedRequest.value.body.toByteArray)
              val batchedRequests =
                tracedBatchedBlockOrderingRequests.requests
                  .map(DbReferenceBlockOrderingStore.fromProto)
              batchedRequests -> CantonTimestamp.ofEpochMicro(
                tracedBatchedBlockOrderingRequests.lastTopologyTimestampEpochMicros
              )
            } else {
              Seq(tracedRequest) -> SignedTopologyTransactionX.InitialTopologySequencingTime
            }
          // Logging the UUID to be able to correlate the block on the write side.
          logger.debug(s"Retrieved block at height $height with UUID: $uuid")
          val blockTimestamp =
            CantonTimestamp.ofEpochMicro(tracedRequest.value.microsecondsSinceEpoch)
          TimestampedBlock(
            BlockOrderer.Block(height, orderedRequests),
            blockTimestamp,
            lastTopologyTimestamp,
          )
        }
      }
}

object DbReferenceBlockOrderingStore {

  def deserializeBytes(
      bytes: Array[Byte]
  ): Either[ProtoDeserializationError, Traced[BlockOrderer.OrderedRequest]] = for {
    protoRequest <- ProtoConverter
      .protoParserArray(proto.TracedBlockOrderingRequest.parseFrom)(bytes)
  } yield fromProto(protoRequest)

  def fromProto(
      protoRequest: proto.TracedBlockOrderingRequest
  ): Traced[BlockOrderer.OrderedRequest] = {
    val request =
      BlockOrderer.OrderedRequest(
        protoRequest.microsecondsSinceEpoch,
        protoRequest.tag,
        protoRequest.body,
      )
    val traceContext = W3CTraceContext(protoRequest.traceparent).toTraceContext
    Traced(request)(traceContext)
  }
}
