// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.util.BatchN
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.chaining.*

class BatchingParallelIngestionPipeSpec
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with PekkoBeforeAndAfterAll {

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  private val input = Iterator.continually(util.Random.nextInt()).take(1000).toList
  // 1000 items must be separated into 10 iterations of 2 parallel batches, so each batch should hold 50 items
  // to hold 50 items with the given weight fn we need a capacity of ~280,  rounding it to 300
  private val MaxBatchSize = 300
  private val MaxTailerBatchSize = 4

  def weightFn(i: Int): Long = i match {
    case n if n % 10 == 0 => 20
    case n if n % 3 == 0 => 10
    case _ => 1
  }

  it should "end the stream successfully in a happy path case" in {
    runPipe().map { case (ingested, ingestedTail, err) =>
      err shouldBe empty
      ingested.sortBy(_._1) shouldBe input.map(_.toString).zipWithIndex.map { case (s, i) =>
        (i + 1, s)
      }
      ingestedTail.last shouldBe 1000
    }
  }

  it should "terminate the stream upon error in input mapper" in {
    runPipe(inputMapperHook = () => throw new Exception("inputmapper failed")).map {
      case (_, _, err) =>
        err.value.getMessage shouldBe "inputmapper failed"
    }
  }

  it should "terminate the stream upon error in seqMapper" in {
    runPipe(seqMapperHook = () => throw new Exception("seqMapper failed")).map { case (_, _, err) =>
      err.value.getMessage shouldBe "seqMapper failed"
    }
  }

  it should "terminate the stream upon error in batcher" in {
    runPipe(batcherHook = () => throw new Exception("batcher failed")).map { case (_, _, err) =>
      err.value.getMessage shouldBe "batcher failed"
    }
  }

  it should "terminate the stream upon error in ingester" in {
    runPipe(ingesterHook = _ => throw new Exception("ingester failed")).map { case (_, _, err) =>
      err.value.getMessage shouldBe "ingester failed"
    }
  }

  it should "terminate the stream upon error in ingestTail" in {
    runPipe(ingestTailHook = _ => throw new Exception("ingestTail failed")).map {
      case (_, _, err) =>
        err.value.getMessage shouldBe "ingestTail failed"
    }
  }

  it should "hold the stream if a single ingestion takes too long and timeouts" in {
    val ingestedTailAcc = new AtomicInteger(0)
    runPipe(
      inputMapperHook = () => Threading.sleep(1L),
      ingesterHook = batch => {
        // due to timing issues it can be that other than full batches are formed, so we check if the batch contains 201
        val max = batch.map(_._1).max
        if (max < 201) {
          ingestedTailAcc.accumulateAndGet(max, _ max _)
        }
        if (batch.map(_._1).contains(201)) Threading.sleep(1000)
      },
      timeout = FiniteDuration(100, "milliseconds"),
    ).map { case (ingested, ingestedTail, err) =>
      err.value.getMessage shouldBe "timed out"
      // worst case: 200 elements will be ingested before 201
      // + 300 elements in the batch that does not contain it and is back-pressured by the batch containing 201
      ingested.size should be <= 500
      ingestedTail.last should be < 201
      ingestedTail.last shouldBe ingestedTailAcc.get()
    }
  }

  it should "form max-sized batches when back-pressured by downstream" in {
    val batchWeights = ArrayBuffer.empty[Long]
    runPipe(
      // Back-pressure to ensure formation of max batch sizes (of size 5)
      inputMapperHook = () => Threading.sleep(1),
      ingesterHook = batch => {
        blocking(batchWeights.synchronized {
          batchWeights.addOne(batch.map(p => weightFn(p._2.toInt)).sum)
        })
        ()
      },
      inputSource = Source(Iterator.continually(util.Random.nextInt()).take(1000).toList),
    ).map { case (_, _, err) =>
      // The first and last batches can be much smaller than `MaxBatchWeight`
      // so we drop 2 and assert the average batch weight instead of the weight of individual batches
      val measurementBatchWeights = batchWeights.drop(2)
      measurementBatchWeights.sum.toDouble / measurementBatchWeights.size should be > (MaxBatchSize.toDouble * 0.7)
      err shouldBe empty
    }
  }

  it should "form small batch sizes under no load" in {
    runPipe(
      ingesterHook = batch => {
        batch.size should be <= 2
        ()
      },
      inputSource = Source(input).take(10).map(_.tap(_ => Threading.sleep(10L))).async,
    ).map { case (ingested, _, err) =>
      err shouldBe empty
      ingested.size shouldBe 10
    }
  }

  it should "form big batch sizes of batches before ingestTail under load" in {
    val batchSizes = ArrayBuffer.empty[Int]

    runPipe(
      ingestTailHook = { batchOfBatches =>
        // Slow ingest tail
        Threading.sleep(20L)
        batchSizes.addOne(batchOfBatches.size)
      },
      inputSource = Source(input).take(100).async,
    ).map { case (_, _, err) =>
      // The first and last batches can be smaller than `MaxTailerBatchSize`
      // so we drop one and assert the average batch size instead of the sizes of individual batches
      val measurementBatchSizes = batchSizes.drop(1)
      measurementBatchSizes.sum.toDouble / measurementBatchSizes.size should be > (MaxTailerBatchSize.toDouble * 0.7)
      err shouldBe empty
    }
  }

  it should "form small batch sizes of batches before ingestTail under no load" in {
    val batchSizes = ArrayBuffer.empty[Int]

    runPipe(
      ingestTailHook = { batchOfBatches => batchSizes.addOne(batchOfBatches.size) },
      inputSource = Source(input)
        .take(100)
        .map(
          _.tap(_ =>
            // Slow down source to ensure ingestTail is faster
            Threading.sleep(1L)
          )
        )
        .async,
    ).map { case (_, _, err) =>
      batchSizes.sum.toDouble / batchSizes.size should be < (MaxTailerBatchSize.toDouble * 0.3)
      err shouldBe empty
    }
  }

  def runPipe(
      inputMapperHook: () => Unit = () => (),
      seqMapperHook: () => Unit = () => (),
      batcherHook: () => Unit = () => (),
      ingesterHook: List[(Int, String)] => Unit = _ => (),
      ingestTailHook: Vector[List[(Int, String)]] => Unit = _ => (),
      timeout: FiniteDuration = FiniteDuration(10, "seconds"),
      inputSource: Source[Int, NotUsed] = Source(input),
  ): Future[(Vector[(Int, String)], Vector[Int], Option[Throwable])] = {
    val semaphore = new Object
    var ingested: Vector[(Int, String)] = Vector.empty
    var ingestedTail: Vector[Int] = Vector.empty
    val indexingFlow =
      BatchingParallelIngestionPipe[Int, List[(Int, Int)], List[(Int, String)]](
        batchingFlow = BatchN.weighted(MaxBatchSize.toLong, 2)(weightFn),
        inputMappingParallelism = 2,
        inputMapper = ins =>
          Future {
            inputMapperHook()
            ins.map((0, _)).toList
          },
        seqMapperZero = List((0, 0)),
        seqMapper = (prev, current) => {
          seqMapperHook()
          val lastIndex = prev.last._1
          current.zipWithIndex.map { case ((_, value), index) =>
            (index + lastIndex + 1, value)
          }
        },
        dbPrepareParallelism = 2,
        dbPrepare = inBatch => Future(inBatch),
        batchingParallelism = 2,
        batcher = inBatch =>
          Future {
            batcherHook()
            inBatch.map { case (index, value) =>
              (index, value.toString)
            }
          },
        ingestingParallelism = 2,
        ingester = dbBatch =>
          Future {
            ingesterHook(dbBatch)
            blocking(semaphore.synchronized {
              ingested = ingested ++ dbBatch
            })
            dbBatch
          },
        maxTailerBatchSize = MaxTailerBatchSize,
        ingestTail = dbBatch =>
          Future {
            ingestTailHook(dbBatch)
            blocking(semaphore.synchronized {
              ingestedTail = ingestedTail :+ dbBatch.last.last._1
            })
            dbBatch
          },
      )
    val p = Promise[(Vector[(Int, String)], Vector[Int], Option[Throwable])]()
    val timeoutF = org.apache.pekko.pattern.after(timeout, system.scheduler) {
      Future.failed(new Exception("timed out"))
    }
    val indexingF = inputSource.via(indexingFlow).run().map { _ =>
      blocking(semaphore.synchronized((ingested, ingestedTail, Option.empty[Throwable])))
    }
    timeoutF.onComplete(p.tryComplete)
    indexingF.onComplete(p.tryComplete)

    p.future.recover { case t =>
      blocking(semaphore.synchronized((ingested, ingestedTail, Some(t))))
    }
  }

}
