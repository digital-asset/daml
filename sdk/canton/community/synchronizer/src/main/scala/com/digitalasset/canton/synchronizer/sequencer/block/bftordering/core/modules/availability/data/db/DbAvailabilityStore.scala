// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1 as proto
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.ExecutionContext

class DbAvailabilityStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends AvailabilityStore[PekkoEnv]
    with DbStore {

  import storage.api.*
  private val profile = storage.profile

  implicit object SetSeqBatchId extends SetParameter[Seq[BatchId]] {
    override def apply(v1: Seq[BatchId], pp: PositionedParameters): Unit =
      v1.foreach(setBatchIdParameter(_, pp))
  }

  private implicit def readOrderingRequestBatch: GetResult[OrderingRequestBatch] =
    GetResult { r =>
      ProtoConverter.protoParserArray(proto.Batch.parseFrom)(r.nextBytes()) match {
        case Left(error) =>
          throw new DbDeserializationException(s"Could not deserialize proto request batch: $error")
        case Right(value) =>
          OrderingRequestBatch.fromProto(Some(value)) match {
            case Left(error) =>
              throw new DbDeserializationException(s"Could not parse batch: $error")
            case Right(value) => value
          }
      }
    }

  private implicit val setOrderingRequestBatch: SetParameter[OrderingRequestBatch] =
    (or, pp) => pp.setBytes(or.toProto.toByteArray)

  private implicit def readBatchId: GetResult[BatchId] = GetResult { r =>
    BatchId.fromHexString(r.nextString()) match {
      case Left(error) =>
        throw new DbDeserializationException(s"Could not deserialize hash: $error")
      case Right(batchId: BatchId) =>
        batchId
    }
  }

  import String68.setParameterLengthLimitedString

  private implicit val setBatchIdParameter: SetParameter[BatchId] =
    (batchId, pp) => pp >> batchId.hash.toLengthLimitedHexString

  override def addBatch(
      batchId: BatchId,
      batch: OrderingRequestBatch,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = addBatchActionName(batchId)
    val future = () =>
      storage.performUnlessClosingUSF(name) {

        storage.update_(
          profile match {
            case _: Postgres =>
              sqlu"""insert into ord_availability_batch
                 values ($batchId, $batch)
                 on conflict (id) do nothing"""
            case _: H2 =>
              sqlu"""merge into ord_availability_batch
                 using values ($batchId, $batch) s(id,batch)
                 on ord_availability_batch.id = s.id
                 when not matched then insert values (s.id, s.batch)
                 """
          },
          functionFullName,
        )
      }
    PekkoFutureUnlessShutdown(name, future)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  override def fetchBatches(batches: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] = {

    val name = fetchBatchesActionName
    if (batches.isEmpty) {
      return PekkoFutureUnlessShutdown(
        name,
        () => FutureUnlessShutdown.pure(AvailabilityStore.AllBatches(Seq.empty)),
      )
    }

    val future: () => FutureUnlessShutdown[AvailabilityStore.FetchBatchesResult] =
      () =>
        storage.performUnlessClosingUSF(name) {
          storage
            .query(
              sql"""select id
              from ord_availability_batch
              where id in ($batches#${",?" * (batches.size - 1)})
             """.as[BatchId],
              functionFullName,
            )
            .flatMap { batchesThatWeHave =>
              val missing = batches.toSet.diff(batchesThatWeHave.toSet)

              if (missing.nonEmpty) {
                FutureUnlessShutdown.pure(AvailabilityStore.MissingBatches(missing))
              } else {
                storage
                  .query(
                    sql"""select id, batch
              from ord_availability_batch
              where id in ($batches#${",?" * (batches.size - 1)})
             """.as[(BatchId, OrderingRequestBatch)],
                    functionFullName,
                  )
                  .map { retrievedBatches =>
                    val batchMap = retrievedBatches.toMap
                    AvailabilityStore.AllBatches(batches.map(i => i -> batchMap(i)))
                  }
              }
            }
        }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def gc(staleBatchIds: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): Unit =
    if (staleBatchIds.nonEmpty) {
      val _ = storage.update_(
        sqlu"""delete from ord_availability_batch
                 where id in ($staleBatchIds#${",?" * (staleBatchIds.size - 1)})""",
        functionFullName,
      )
    }
}
