// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.chaining._

class BatchingParallelIngestionPipeSpec
    extends AsyncFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll {

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  private val input = Iterator.continually(util.Random.nextInt()).take(100).toList
  private val MaxBatchSize = 5
  private val MaxTailerBatchSize = 4

  it should "end the stream successfully in a happy path case" in {
    runPipe().map { case (ingested, ingestedTail, err) =>
      err shouldBe empty
      ingested.sortBy(_._1) shouldBe input.map(_.toString).zipWithIndex.map { case (s, i) =>
        (i + 1, s)
      }
      ingestedTail.last shouldBe 100
    }
  }

  it should "terminate the stream upon error in input mapper" in {
    runPipe(inputMapperHook = () => throw new Exception("inputmapper failed")).map {
      case (_, _, err) =>
        err should not be empty
        err.get.getMessage shouldBe "inputmapper failed"
    }
  }

  it should "terminate the stream upon error in seqMapper" in {
    runPipe(seqMapperHook = () => throw new Exception("seqMapper failed")).map { case (_, _, err) =>
      err should not be empty
      err.get.getMessage shouldBe "seqMapper failed"
    }
  }

  it should "terminate the stream upon error in batcher" in {
    runPipe(batcherHook = () => throw new Exception("batcher failed")).map { case (_, _, err) =>
      err should not be empty
      err.get.getMessage shouldBe "batcher failed"
    }
  }

  it should "terminate the stream upon error in ingester" in {
    runPipe(ingesterHook = _ => throw new Exception("ingester failed")).map { case (_, _, err) =>
      err should not be empty
      err.get.getMessage shouldBe "ingester failed"
    }
  }

  it should "terminate the stream upon error in ingestTail" in {
    runPipe(ingestTailHook = _ => throw new Exception("ingestTail failed")).map {
      case (_, _, err) =>
        err should not be empty
        err.get.getMessage shouldBe "ingestTail failed"
    }
  }

  it should "hold the stream if a single ingestion takes too long" in {
    runPipe(
      inputMapperHook = () => Thread.sleep(1L),
      ingesterHook = batch => {
        if (batch.head == 21) Thread.sleep(1000)
      },
      timeout = FiniteDuration(100, "milliseconds"),
    ).map { case (ingested, ingestedTail, err) =>
      err should not be empty
      err.get.getMessage shouldBe "timed out"
      ingested.size shouldBe 25
      ingestedTail.last shouldBe 20
    }
  }

  it should "form max-sized batches when back-pressured by downstream" in {
    val batchSizes = ArrayBuffer.empty[Int]
    runPipe(
      // Back-pressure to ensure formation of max batch sizes (of size 5)
      inputMapperHook = () => Thread.sleep(1),
      ingesterHook = batch => {
        batchSizes.synchronized {
          batchSizes.addOne(batch.size)
        }
        ()
      },
    ).map { case (_, _, err) =>
      // The first and last batches can be smaller than `MaxBatchSize`
      // so we assert the average batch size instead of the sizes of individual batches
      batchSizes.sum.toDouble / batchSizes.size should be > (MaxBatchSize.toDouble * 0.8)
      err shouldBe empty
    }
  }

  it should "form small batch sizes under no load" in {
    runPipe(
      ingesterHook = batch => {
        batch.size shouldBe 1
        ()
      },
      inputSource = Source(input).take(10).map(_.tap(_ => Thread.sleep(10L))).async,
    ).map { case (_, _, err) =>
      err shouldBe empty
    }
  }

  it should "form big batch sizes of batches before ingestTail under load" in {
    val batchSizes = ArrayBuffer.empty[Int]

    runPipe(
      ingestTailHook = { batchOfBatches =>
        // Slow ingest tail
        Thread.sleep(10L)
        batchSizes.addOne(batchOfBatches.size)
      },
      inputSource = Source(input).take(100).async,
    ).map { case (_, _, err) =>
      // The first and last batches can be smaller than `MaxTailerBatchSize`
      // so we assert the average batch size instead of the sizes of individual batches
      batchSizes.sum.toDouble / batchSizes.size should be > (MaxTailerBatchSize.toDouble * 0.7)
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
            Thread.sleep(1L)
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
      ingesterHook: List[Int] => Unit = _ => (),
      ingestTailHook: Vector[List[(Int, String)]] => Unit = _ => (),
      timeout: FiniteDuration = FiniteDuration(10, "seconds"),
      inputSource: Source[Int, NotUsed] = Source(input),
  ): Future[(Vector[(Int, String)], Vector[Int], Option[Throwable])] = {
    val semaphore = new Object
    var ingested: Vector[(Int, String)] = Vector.empty
    var ingestedTail: Vector[Int] = Vector.empty
    val indexingSource: Source[Int, NotUsed] => Source[List[(Int, String)], NotUsed] =
      BatchingParallelIngestionPipe[Int, List[(Int, Int)], List[(Int, String)]](
        submissionBatchSize = MaxBatchSize.toLong,
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
            ingesterHook(dbBatch.map(_._1))
            semaphore.synchronized {
              ingested = ingested ++ dbBatch
            }
            dbBatch
          },
        maxTailerBatchSize = MaxTailerBatchSize,
        ingestTail = dbBatch =>
          Future {
            ingestTailHook(dbBatch)
            semaphore.synchronized {
              ingestedTail = ingestedTail :+ dbBatch.last.last._1
            }
            dbBatch
          },
      )
    val p = Promise[(Vector[(Int, String)], Vector[Int], Option[Throwable])]()
    val timeoutF = akka.pattern.after(timeout, system.scheduler) {
      Future.failed(new Exception("timed out"))
    }
    val indexingF = indexingSource(inputSource).run().map { _ =>
      (ingested, ingestedTail, Option.empty[Throwable])
    }
    timeoutF.onComplete(p.tryComplete)
    indexingF.onComplete(p.tryComplete)

    p.future.recover { case t =>
      (ingested, ingestedTail, Some(t))
    }
  }
}
