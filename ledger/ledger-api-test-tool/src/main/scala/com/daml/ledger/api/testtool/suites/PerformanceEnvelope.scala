// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.time.{Duration, Instant}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  Allocation,
  Assertions,
  Envelope,
  LedgerTestSuite,
}
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{GetTransactionsRequest, GetTransactionsResponse}
import com.daml.ledger.client.binding.{Primitive => P}
import com.daml.ledger.test.performance.{PingPong => PingPongModule}
import io.grpc.stub.StreamObserver
import io.grpc.{Context, Status}
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.tag._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Random, Success, Try}

sealed trait PerformanceEnvelope[E <: Envelope] {

  protected def logger: Logger
  protected def envelope: E
  protected def maxInflight: Int

  protected def waitForParties(participants: Seq[Allocation.Participant]): Unit = {
    val (participantAlice, alice) = (participants.head.ledger, participants.head.parties.head)
    val (participantBob, bob) = (participants(1).ledger, participants(1).parties.head)
    val _ = participantAlice.waitForParties(Seq(participantBob), Set(alice, bob))
  }

  /** swiss army knife for setting up envelope tests
    *
    * This function sends a series of pings from one participant to another,
    * using specific workflow ids to measure progress.
    *
    * The maxInflight parameter controls how many pings are on the way. In throughput tests,
    * we use it for back-pressure as most platforms don't support backpressue yet.
    *
    * The return value is the time it takes us to run all pings and the list of individual ping
    * times.
    *
    * The payload string is the string that we put on every ping.
    *
    * Therefore, this function allows us to test
    * - the max transaction size (1 ping with payload = 5kb string)
    * - throughput (e.g. 200 pings with 40 in-flight)
    * - latency (20 pings with 1 in-flight)
    */
  protected def sendPings(
      from: Participant,
      to: Participant,
      workflowIds: List[String],
      payload: String,
  )(implicit ec: ExecutionContext): Future[(Duration, List[Duration])] = {

    val (participantAlice, alice) = (from.ledger, from.parties.head)
    val (participantBob, bob) = (to.ledger, to.parties.head)
    val queued = new ConcurrentLinkedQueue[Promise[Unit]]()
    val inflight = new AtomicInteger(0)
    // used to track the duration of each ping (left is start time, right is elapsed once we know end-time)
    val timings = TrieMap[String, Either[Instant, Duration]]()
    val tracker = Promise[Either[String, Unit]]()

    def sendPing(workflowId: String): Future[Unit] = {

      val promise = Promise[Unit]()
      queued.add(promise)

      // start one immediately (might be some other task we start)
      if (inflight.incrementAndGet() <= maxInflight) {
        Option(queued.poll()).foreach(_.success(()))
      }
      for {
        // wait for our turn
        _ <- blocking { promise.future }
        // build request
        request = submitRequest(
          participantAlice,
          alice,
          PingPongModule.Ping(payload, alice, List(bob)).create.command,
          workflowId,
        )
        _ = {
          logger.info(s"Submitting ping with workflowId=$workflowId")
          timings += workflowId -> Left(Instant.now)
        }
        // and submit it
        _ <- participantAlice.submit(request)
      } yield ()
    }

    val awaiter = waitForAllTransactions(
      tracker,
      participantBob,
      bob,
      workflowIds.length,
      queued,
      inflight,
      timings,
    )
    for {
      end <- participantAlice.completionEnd(CompletionEndRequest(participantAlice.ledgerId))
      _ = listenCompletions(tracker, participantAlice, alice, end.offset)
      started = Instant.now
      _ <- Future.traverse(workflowIds)(sendPing)
      res <- awaiter
    } yield {
      res match {
        case Left(err) => Assertions.fail(err)
        case Right(_) =>
          val finished = Instant.now
          (
            Duration.between(started, finished),
            timings.values.flatMap(_.toOption.toList).toList,
          )
      }
    }
  }

  private def submitRequest(
      participant: ParticipantTestContext,
      party: P.Party,
      command: Command,
      commandAndWorkflowId: String,
  ) = {
    new SubmitRequest(
      Some(
        new Commands(
          ledgerId = participant.ledgerId,
          applicationId = participant.applicationId,
          commandId = commandAndWorkflowId,
          workflowId = commandAndWorkflowId,
          party = party.unwrap,
          commands = Seq(command),
        )
      )
    )
  }

  private def waitForAllTransactions(
      observedAll: Promise[Either[String, Unit]],
      participant: ParticipantTestContext,
      party: P.Party,
      numPings: Int,
      queue: ConcurrentLinkedQueue[Promise[Unit]],
      inflight: AtomicInteger,
      timings: TrieMap[String, Either[Instant, Duration]],
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] = {

    val observed = new AtomicInteger(0)
    val context = Context.ROOT.withCancellation()

    for {
      offset <- participant.currentEnd()
    } yield {
      context.run(() =>
        participant.transactionStream(
          GetTransactionsRequest(
            ledgerId = participant.ledgerId,
            begin = Some(offset),
            end = None,
            verbose = false,
            filter = Some(
              TransactionFilter(filtersByParty = Map(party.unwrap -> Filters(inclusive = None)))
            ),
          ),
          new StreamObserver[GetTransactionsResponse] {
            // find workflow ids and signal if we observed all expected
            @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
            override def onNext(value: GetTransactionsResponse): Unit = {
              value.transactions.foreach { tr: Transaction =>
                timings.get(tr.workflowId) match {
                  case Some(Left(started)) =>
                    val finished = Instant.now()
                    val inf = inflight.decrementAndGet()
                    val obs = observed.incrementAndGet()
                    // start next ping
                    Option(queue.poll()).foreach(_.success(()))
                    logger.info(s"Observed ping ${tr.workflowId} (observed=$obs, inflight=$inf)")
                    timings.update(tr.workflowId, Right(Duration.between(started, finished)))
                    // signal via future that we are done
                    if (observed.get() == numPings && !observedAll.isCompleted)
                      observedAll.trySuccess(Right(()))
                  // there shouldn't be running anything concurrently
                  case None =>
                    logger.error(
                      s"Observed transaction with un-expected workflowId ${tr.workflowId}"
                    )
                  case Some(Right(_)) =>
                    logger.error(s"Observed transaction with workflowId ${tr.workflowId} twice!")
                }
              }
            }

            override def onError(t: Throwable): Unit = t match {
              case ex: io.grpc.StatusRuntimeException
                  if ex.getStatus.getCode == io.grpc.Status.CANCELLED.getCode =>
              case _ => logger.error("GetTransactionResponse stopped due to an error", t)
            }

            override def onCompleted(): Unit = {
              if (observed.get() != numPings) {
                logger.error(
                  s"Transaction stream closed before I've observed all transactions. Missing are ${numPings - observed
                    .get()}."
                )
              }

            }
          },
        )
      )
    }
    // ensure we cancel the stream once we've observed everything
    observedAll.future.map { x =>
      Try(context.cancel(Status.CANCELLED.asException())) match {
        case Success(_) => ()
        case Failure(ex) =>
          logger.error("Cancelling transaction stream failed with an exception", ex)
      }
      x
    }
  }

  private def listenCompletions(
      tracker: Promise[Either[String, Unit]],
      sender: ParticipantTestContext,
      party: P.Party,
      offset: Option[LedgerOffset],
  )(implicit ec: ExecutionContext): Unit = {
    val context = Context.ROOT.withCancellation()

    context.run(() =>
      sender.completionStream(
        CompletionStreamRequest(
          ledgerId = sender.ledgerId,
          applicationId = sender.applicationId,
          parties = Seq(party.unwrap),
          offset = offset,
        ),
        new StreamObserver[CompletionStreamResponse] {
          @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
          override def onNext(value: CompletionStreamResponse): Unit = {
            value.completions.foreach { completion =>
              completion.status.foreach { status =>
                // TODO(rv) maybe, add re-submission logic once systems are smart enough to back-pressure
                if (status.code != 0) {
                  if (status.code == io.grpc.Status.DEADLINE_EXCEEDED.getCode.value()) {
                    logger.error(
                      s"Command ${completion.commandId} timed-out. You might want to reduce the number of in-flight commands. $status"
                    )
                  } else {
                    logger.error(s"Command ${completion.commandId} failed with $status")
                  }
                  // for now, we kill the test if we hit an error
                  tracker.trySuccess(Left(s"Command ${completion.commandId} failed with $status"))
                } else {
                  logger.debug(s"Command ${completion.commandId} succeeded")
                }
              }
            }
          }
          override def onError(t: Throwable): Unit = {}
          override def onCompleted(): Unit = {}
        },
      )
    )
    tracker.future.map(_ => Try(context.cancel(Status.CANCELLED.asException())))
    ()
  }

}

object PerformanceEnvelope {

  def apply[E <: Envelope](
      envelope: E,
      reporter: (String, Double) => Unit,
  ): LedgerTestSuite =
    envelope match {
      case e: Envelope.Latency =>
        new LatencyTest(e, numPings = e.numPings, numWarmupPings = e.numPings, reporter = reporter)
      case e: Envelope.Throughput =>
        val numPings = 100000
           // test should run at least 15 seconds
        new ThroughputTest(
          envelope = e,
          maxInflight = e.operationsPerSecond * 5, // aiming for a latency of 5 seconds
          numPings = numPings,
          numWarmupPings = numPings,
          reporter = reporter,
        )
      case e: Envelope.TransactionSize => new TransactionSizeScaleTest(e)
    }

  /** Throughput test
    *
    * @param numPings  how many pings to run during the throughput test
    * @param maxInflight how many inflight commands we can have. set it high enough such that the system saturates, keep it low enough to not hit timeouts.
    * @param numWarmupPings how many pings to run before the perf test to warm up the system
    */
  private final class ThroughputTest(
      override protected val envelope: Envelope.Throughput,
      override protected val maxInflight: Int,
      numPings: Int,
      numWarmupPings: Int,
      reporter: (String, Double) => Unit,
  ) extends LedgerTestSuite
      with PerformanceEnvelope[Envelope.Throughput] {

    override protected val logger: Logger = LoggerFactory.getLogger(getClass)

    test(
      envelope.name.replace(".", ""),
      s"Verify that ledger passes the ${envelope.name} throughput envelope",
      allocate(SingleParty, SingleParty),
    )(implicit ec => { case participants =>
      waitForParties(participants.participants)

      def runTest(num: Int, description: String): Future[(Duration, List[Duration])] =
        sendPings(
          from = participants.participants.head,
          to = participants.participants(1),
          workflowIds = (1 to num).map(x => s"$description-$x").toList,
          payload = description,
        )
      for {
        _ <- runTest(numWarmupPings, "throughput-warmup")
        timings <- runTest(numPings, "throughput-test")
      } yield {
        val (elapsed, latencies) = timings
        val throughput = numPings / elapsed.toMillis.toDouble * 1000.0
        logger.info(
          s"Sending of $numPings succeeded after $elapsed, yielding a throughput of ${"%.2f" format throughput}."
        )
        reporter("rate", throughput)
        logger.info(
          s"Throughput latency stats: ${genStats(latencies.map(_.toMillis), (_, _) => ())}"
        )
        assert(
          throughput >= envelope.operationsPerSecond,
          s"Observed throughput of ${"%.2f" format throughput} is below the necessary envelope level ${envelope.operationsPerSecond}",
        )
      }
    })
  }

  private final class LatencyTest(
      override protected val envelope: Envelope.Latency,
      numPings: Int,
      numWarmupPings: Int,
      reporter: (String, Double) => Unit,
  ) extends LedgerTestSuite
      with PerformanceEnvelope[Envelope.Latency] {

    override protected val logger: Logger = LoggerFactory.getLogger(getClass)

    override protected val maxInflight = 1 // will only be one
    require(numPings > 0 && numWarmupPings >= 0)

    test(
      envelope.name.replace(".", ""),
      s"Verify that ledger passes the ${envelope.name} latency envelope",
      allocate(SingleParty, SingleParty),
    )(implicit ec => { case participants =>
      waitForParties(participants.participants)

      sendPings(
        from = participants.participants.head,
        to = participants.participants(1),
        workflowIds = (1 to (numPings + numWarmupPings)).map(x => s"latency-$x").toList,
        payload = "latency",
      ).map { case (_, latencies) =>
        val sample = latencies.drop(numWarmupPings).map(_.toMillis).sorted
        require(sample.length == numPings)
        val tailCount = sample.count(_ > envelope.latency.toMillis)
        val stats = genStats(sample, reporter)
        logger.info(s"Latency test finished: $stats")
        assert(
          tailCount <= numPings * 0.1,
          s"$tailCount out of $numPings are above the latency threshold. Stats are $stats",
        )
      }
    })
  }

  private def genStats(sample: List[Long], reporter: (String, Double) => Unit): String = {
    val num = sample.length.toDouble
    val avg = sample.sum / num
    val med = sample(sample.length / 2)
    val stddev = Math.sqrt(sample.map(x => (x - avg) * (x - avg)).sum / num)
    reporter("average", avg)
    reporter("median", med.toDouble)
    reporter("stddev", stddev)
    s"Sample size of ${sample.length}: avg=${"%.0f" format avg} ms, median=$med ms, stdev=${"%.0f" format stddev} ms"
  }

  private final class TransactionSizeScaleTest(
      override protected val envelope: Envelope.TransactionSize
  ) extends LedgerTestSuite
      with PerformanceEnvelope[Envelope.TransactionSize] {

    override protected val logger: Logger = LoggerFactory.getLogger(getClass)
    override protected val maxInflight = 10

    test(
      envelope.name.replace(".", ""),
      s"Verify that ledger passes the ${envelope.name} transaction size envelope",
      allocate(SingleParty, SingleParty),
    )(implicit ec => { case participants =>
      waitForParties(participants.participants)

      sendPings(
        from = participants.participants.head,
        to = participants.participants(1),
        workflowIds = List("transaction-size"),
        payload = Random.alphanumeric.take(envelope.kilobytes * 1024).mkString(""),
      ).map(_ => ())
    })
  }

}
