// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.configuration.{Configuration, LedgerInitialConditions}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.KVOffsetBuilder
import com.daml.ledger.participant.state.kvutils.app.RunnerSpec._
import com.daml.ledger.participant.state.v2.{
  PruningResult,
  ReadService,
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
  Update,
  WriteService,
}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.{Dispatcher, SubSource}
import com.daml.ports.Port
import com.daml.telemetry.TelemetryContext
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status.Code
import io.grpc.{Channel, ManagedChannelBuilder, Status}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials

class RunnerSpec extends AsyncWordSpec with Matchers {
  private implicit val resourceContext: ResourceContext = ResourceContext(ExecutionContext.global)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "the runner" should {
    "start a participant" in {
      val config = Config.createDefault(()).copy(ledgerId = LedgerId)
      val participantConfig = newTestPartcipantConfig()

      val runner = new Runner(Name, TestLedgerFactory, ConfigProvider.ForUnit)
      val app = startWithRunner(config, participantConfig, runner)

      app.asFuture
        .flatMap { channel =>
          LedgerIdentityServiceGrpc
            .stub(channel)
            .getLedgerIdentity(GetLedgerIdentityRequest.of())
            .map { response =>
              response.ledgerId should be(LedgerId)
            }
        }
        .transformWith(result => app.release().flatMap(_ => Future.fromTry(result)))
    }
  }
}

object RunnerSpec {
  private val Name = classOf[RunnerSpec].getSimpleName
  private val LedgerId = s"$Name-Ledger"

  private val logger = ContextualizedLogger.get(getClass)

  private val engine = Engine.StableEngine()

  private def newTestPartcipantConfig() = {
    val participantId = Ref.ParticipantId.assertFromString("participant")
    ParticipantConfig(
      mode = ParticipantRunMode.Combined,
      participantId = participantId,
      shardName = None,
      address = None,
      port = Port.Dynamic,
      portFile = None,
      serverJdbcUrl = ParticipantConfig.defaultIndexJdbcUrl(participantId),
      indexerConfig = ParticipantIndexerConfig(allowExistingSchema = false),
    )
  }

  private def startWithRunner(
      config: Config[Unit],
      participantConfig: ParticipantConfig,
      runner: Runner[Nothing, Unit],
  )(implicit
      resourceContext: ResourceContext,
      loggingContext: LoggingContext,
  ): Resource[Channel] = {
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(Name)).acquire()
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem)).acquire()
      port <- runner
        .runParticipant(config, participantConfig, engine)(
          resourceContext,
          loggingContext,
          actorSystem,
          materializer,
        )
        .map(_.get)
      channel <- ResourceOwner
        .forChannel(
          {
            val builder = ManagedChannelBuilder
              .forAddress(InetAddress.getLoopbackAddress.getHostName, port.value)
            builder.usePlaintext()
            builder
          },
          shutdownTimeout = 1.second,
        )
        .acquire()
    } yield channel
  }

  object TestLedgerFactory extends LedgerFactory[Unit] {
    private val offsetBuilder = new KVOffsetBuilder(0)

    private val initialConditions = LedgerInitialConditions(
      LedgerId,
      Configuration.reasonableInitialConfiguration,
      Timestamp.Epoch,
    )

    override def readWriteServiceFactoryOwner(
        config: Config[Unit],
        participantConfig: ParticipantConfig,
        engine: Engine,
        metrics: Metrics,
    )(implicit
        materializer: Materializer,
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): ResourceOwner[ReadWriteServiceFactory] =
      for {
        updates <- ResourceOwner.forValue(() => mutable.ArrayBuffer.empty[Update])
        head = new AtomicInteger(0)
        dispatcher <- Dispatcher.owner(Name, 0, head.get())
        updateQueue <- ResourceOwner
          .forBoundedSourceQueue(
            Source
              .queue[(Int, Update)](bufferSize = 100)
              .toMat(Sink.foreach { case (head, update) =>
                updates += update
                dispatcher.signalNewHead(head)
              })(Keep.both)
          )
          .map(_._1)
        factory <- ResourceOwner.successful(new ReadWriteServiceFactory {
          override def readService(): ReadService = new ReadService {
            override def currentHealth(): HealthStatus = Healthy

            override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
              Source.single(initialConditions).concat(Source.never)

            override def stateUpdates(
                beginAfter: Option[Offset]
            )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] =
              dispatcher
                .startingAt(
                  beginAfter.fold(0)(offsetBuilder.highestIndex(_).toInt),
                  SubSource.OneAfterAnother(
                    _ + 1,
                    index => Future.successful(updates(index - 1)),
                  ),
                )
                .map { case (index, update) =>
                  offsetBuilder.of(index.toLong) -> update
                }
          }

          override def writeService(): WriteService = new WriteService {
            override def currentHealth(): HealthStatus = Healthy

            override def submitConfiguration(
                maxRecordTime: Timestamp,
                submissionId: Ref.SubmissionId,
                config: Configuration,
            )(implicit
                loggingContext: LoggingContext,
                telemetryContext: TelemetryContext,
            ): CompletionStage[SubmissionResult] = {
              val configurationUpdate = Update.ConfigurationChanged(
                recordTime = Timestamp.now(),
                submissionId = submissionId,
                participantId = participantConfig.participantId,
                newConfiguration = config,
              )
              updateQueue.offer(head.incrementAndGet() -> configurationUpdate) match {
                case QueueOfferResult.Enqueued =>
                  acknowledged
                case QueueOfferResult.Dropped =>
                  failure(Code.RESOURCE_EXHAUSTED, "submitConfiguration")
                case QueueOfferResult.QueueClosed =>
                  failure(Code.ABORTED, "submitConfiguration")
                case QueueOfferResult.Failure(cause) =>
                  logger.error("submitConfiguration", cause)
                  failure(Code.INTERNAL, "submitConfiguration")
              }
            }

            override def allocateParty(
                hint: Option[Ref.Party],
                displayName: Option[String],
                submissionId: Ref.SubmissionId,
            )(implicit
                loggingContext: LoggingContext,
                telemetryContext: TelemetryContext,
            ): CompletionStage[SubmissionResult] = failure(Code.UNIMPLEMENTED, "allocateParty")

            override def uploadPackages(
                submissionId: Ref.SubmissionId,
                archives: List[DamlLf.Archive],
                sourceDescription: Option[String],
            )(implicit
                loggingContext: LoggingContext,
                telemetryContext: TelemetryContext,
            ): CompletionStage[SubmissionResult] = failure(Code.UNIMPLEMENTED, "uploadPackages")

            override def submitTransaction(
                submitterInfo: SubmitterInfo,
                transactionMeta: TransactionMeta,
                transaction: SubmittedTransaction,
                estimatedInterpretationCost: Long,
            )(implicit
                loggingContext: LoggingContext,
                telemetryContext: TelemetryContext,
            ): CompletionStage[SubmissionResult] = failure(Code.UNIMPLEMENTED, "submitTransaction")

            override def prune(
                pruneUpToInclusive: Offset,
                submissionId: Ref.SubmissionId,
                pruneAllDivulgedContracts: Boolean,
            ): CompletionStage[PruningResult] = CompletableFuture.completedFuture(
              PruningResult.NotPruned(Status.UNIMPLEMENTED.withDescription("prune"))
            )
          }
        })
      } yield factory

    private def acknowledged: CompletionStage[SubmissionResult] =
      CompletableFuture.completedFuture(SubmissionResult.Acknowledged)

    private def failure(code: Code, message: String): CompletionStage[SubmissionResult] =
      CompletableFuture.completedFuture(
        SubmissionResult.SynchronousError(StatusProto.of(code.value(), message, Seq.empty))
      )
  }
}
