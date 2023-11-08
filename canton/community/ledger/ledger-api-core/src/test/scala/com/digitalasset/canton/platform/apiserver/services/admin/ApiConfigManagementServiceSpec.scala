// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.RetryInfoDetail
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.config_management_service.{
  GetTimeModelRequest,
  SetTimeModelRequest,
  TimeModel,
}
import com.daml.lf.data.Ref.SubmissionId
import com.daml.lf.data.{Ref, Time}
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.canton.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.ledger.participant.state.v2.{
  SubmissionResult,
  WriteConfigService,
  WriteService,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.admin.ApiConfigManagementServiceSpec.*
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.{BaseTest, DiscardOps}
import com.google.protobuf.duration.Duration as DurationProto
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterEach, Inside}

import java.time.Duration
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.immutable
import scala.concurrent.duration.{Duration as ScalaDuration, DurationInt}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

class ApiConfigManagementServiceSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ArgumentMatchersSugar
    with PekkoBeforeAndAfterAll
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach {

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  "ApiConfigManagementService" should {
    "get the time model" in {
      val indexedTimeModel = LedgerTimeModel(
        avgTransactionLatency = Duration.ofMinutes(5),
        minSkew = Duration.ofMinutes(3),
        maxSkew = Duration.ofMinutes(2),
      ).get
      val expectedTimeModel = TimeModel.of(
        avgTransactionLatency = Some(DurationProto.of(5 * 60, 0)),
        minSkew = Some(DurationProto.of(3 * 60, 0)),
        maxSkew = Some(DurationProto.of(2 * 60, 0)),
      )

      val writeService = mock[state.WriteConfigService]
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        new FakeCurrentIndexConfigManagementService(
          LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")),
          Configuration(aConfigurationGeneration, indexedTimeModel, Duration.ZERO),
        ),
        writeService,
        TimeProvider.UTC,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      apiConfigManagementService
        .getTimeModel(GetTimeModelRequest.defaultInstance)
        .map { response =>
          response.timeModel should be(Some(expectedTimeModel))
          verifyZeroInteractions(writeService)
          succeed
        }
    }

    "return a `NOT_FOUND` error if a time model is not found" in {
      val writeService = mock[WriteConfigService]
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        EmptyIndexConfigManagementService,
        writeService,
        TimeProvider.UTC,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      apiConfigManagementService
        .getTimeModel(GetTimeModelRequest.defaultInstance)
        .transform(Success.apply)
        .map { response =>
          response should matchPattern { case Failure(GrpcException(GrpcStatus.NOT_FOUND(), _)) =>
          }
        }
    }

    "set a new time model" in {
      val maximumDeduplicationDuration = Duration.ofHours(6)
      val initialGeneration = 2L
      val initialTimeModel = LedgerTimeModel(
        avgTransactionLatency = Duration.ofMinutes(1),
        minSkew = Duration.ofMinutes(2),
        maxSkew = Duration.ofMinutes(3),
      ).get
      val initialConfiguration = Configuration(
        generation = initialGeneration,
        timeModel = initialTimeModel,
        maxDeduplicationDuration = maximumDeduplicationDuration,
      )
      val expectedGeneration = 3L
      val expectedTimeModel = LedgerTimeModel(
        avgTransactionLatency = Duration.ofMinutes(2),
        minSkew = Duration.ofMinutes(1),
        maxSkew = Duration.ofSeconds(30),
      ).get
      val expectedConfiguration = Configuration(
        generation = expectedGeneration,
        timeModel = expectedTimeModel,
        maxDeduplicationDuration = maximumDeduplicationDuration,
      )

      val timeProvider = TimeProvider.UTC
      val maximumRecordTime = timeProvider.getCurrentTime.plusSeconds(60)

      val (indexService, writeService, currentConfiguration) = bridgedServices(
        startingOffset = 7,
        submissions = Seq(Ref.SubmissionId.assertFromString("one") -> initialConfiguration),
      )
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        indexService,
        writeService,
        timeProvider,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      apiConfigManagementService
        .setTimeModel(
          SetTimeModelRequest.of(
            submissionId = "some submission ID",
            maximumRecordTime = Some(Timestamp.of(maximumRecordTime.getEpochSecond, 0)),
            configurationGeneration = initialGeneration,
            newTimeModel = Some(
              TimeModel(
                avgTransactionLatency = Some(DurationProto.of(2 * 60, 0)),
                minSkew = Some(DurationProto.of(60, 0)),
                maxSkew = Some(DurationProto.of(30, 0)),
              )
            ),
          )
        )
        .map { response =>
          response.configurationGeneration should be(expectedGeneration)
          currentConfiguration() should be(Some(expectedConfiguration))
          succeed
        }
    }

    "refuse to set a new time model if none is indexed" in {
      val initialGeneration = 0L

      val timeProvider = TimeProvider.UTC
      val maximumRecordTime = timeProvider.getCurrentTime.plusSeconds(60)

      val writeService = mock[WriteService]
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        EmptyIndexConfigManagementService,
        writeService,
        timeProvider,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      loggerFactory.assertLogs(
        within = {
          apiConfigManagementService
            .setTimeModel(
              SetTimeModelRequest.of(
                "a submission ID",
                maximumRecordTime = Some(Timestamp.of(maximumRecordTime.getEpochSecond, 0)),
                configurationGeneration = initialGeneration,
                newTimeModel = Some(
                  TimeModel(
                    avgTransactionLatency = Some(DurationProto.of(10, 0)),
                    minSkew = Some(DurationProto.of(20, 0)),
                    maxSkew = Some(DurationProto.of(40, 0)),
                  )
                ),
              )
            )
            .transform(Success.apply)
            .map { response =>
              verifyZeroInteractions(writeService)
              response should matchPattern {
                case Failure(GrpcException(GrpcStatus.NOT_FOUND(), _)) =>
              }
            }
        },
        assertions = _.warningMessage should include("Could not get the current time model."),
      )
    }

    "propagate trace context" in {
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        new FakeStreamingIndexConfigManagementService(someConfigurationEntries),
        TestWriteConfigService(testTelemetrySetup.tracer),
        TimeProvider.UTC,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
        telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
        loggerFactory = loggerFactory,
      )

      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      apiConfigManagementService
        .setTimeModel(aSetTimeModelRequest)
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          testTelemetrySetup.reportedSpanAttributes should contain(anApplicationIdSpanAttribute)
          succeed
        }
    }

    "close while setting time model" in {
      val writeService = mock[state.WriteConfigService]
      when(
        writeService.submitConfiguration(
          any[Time.Timestamp],
          any[Ref.SubmissionId],
          any[Configuration],
        )(any[TraceContext])
      ).thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))

      val indexConfigManagementService = new FakeCurrentIndexConfigManagementService(
        LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")),
        aConfiguration,
      )

      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        indexConfigManagementService,
        writeService,
        TimeProvider.UTC,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      indexConfigManagementService.getNextConfigurationEntriesPromise.future
        .map(_ => apiConfigManagementService.close())
        .discard

      apiConfigManagementService
        .setTimeModel(
          aSetTimeModelRequest
        )
        .transform {
          case Success(_) =>
            fail("Expected a failure, but received success")
          case Failure(err: StatusRuntimeException) =>
            assertError(
              actual = err,
              expectedStatusCode = Code.UNAVAILABLE,
              expectedMessage = "SERVER_IS_SHUTTING_DOWN(1,0): Server is shutting down",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "SERVER_IS_SHUTTING_DOWN",
                  Map(
                    "submissionId" -> s"'$aSubmissionId'",
                    "category" -> "1",
                    "definite_answer" -> "false",
                    "test" -> s"'${getClass.getSimpleName}'",
                  ),
                ),
                RetryInfoDetail(1.second),
              ),
              verifyEmptyStackTrace = true,
            )
            Success(succeed)
          case Failure(other) =>
            fail("Unexpected error", other)
        }
    }
  }
}

object ApiConfigManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private val aConfigurationGeneration = 0L

  private val aConfiguration = Configuration(
    aConfigurationGeneration,
    LedgerTimeModel.reasonableDefault,
    Duration.ZERO,
  )

  private val someConfigurationEntries = List(
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")) ->
      ConfigurationEntry.Accepted(
        aSubmissionId,
        aConfiguration,
      )
  )

  private val aSetTimeModelRequest = SetTimeModelRequest(
    aSubmissionId,
    Some(Timestamp.of(TimeProvider.UTC.getCurrentTime.plusSeconds(600).getEpochSecond, 0)),
    aConfigurationGeneration,
    Some(
      TimeModel(
        Some(DurationProto.defaultInstance),
        Some(DurationProto.defaultInstance),
        Some(DurationProto.defaultInstance),
      )
    ),
  )

  private object EmptyIndexConfigManagementService extends IndexConfigManagementService {
    override def lookupConfiguration()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
      Future.successful(None)

    override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] =
      Source.never
  }

  private final class FakeCurrentIndexConfigManagementService(
      offset: LedgerOffset.Absolute,
      configuration: Configuration,
  ) extends IndexConfigManagementService {
    override def lookupConfiguration()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
      Future.successful(Some(offset -> configuration))

    override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] = {
      nextConfigurationEntriesPromise.getAndSet(Promise[Unit]()).success(())
      Source.never
    }

    private val nextConfigurationEntriesPromise =
      new AtomicReference[Promise[Unit]](Promise[Unit]())
    def getNextConfigurationEntriesPromise: Promise[Unit] = nextConfigurationEntriesPromise.get()
  }

  private final class FakeStreamingIndexConfigManagementService(
      entries: immutable.Iterable[(LedgerOffset.Absolute, ConfigurationEntry)]
  ) extends IndexConfigManagementService {
    private val currentConfiguration =
      entries.collect { case (offset, ConfigurationEntry.Accepted(_, configuration)) =>
        offset -> configuration
      }.lastOption

    override def lookupConfiguration()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
      Future.successful(currentConfiguration)

    override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] =
      Source(entries)
  }

  private final case class TestWriteConfigService(tracer: Tracer) extends state.WriteConfigService {
    override def submitConfiguration(
        maxRecordTime: Time.Timestamp,
        submissionId: Ref.SubmissionId,
        config: Configuration,
    )(implicit
        traceContext: TraceContext
    ): CompletionStage[state.SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      completedFuture(state.SubmissionResult.Acknowledged)
    }
  }

  private def bridgedServices(
      startingOffset: Long,
      submissions: Iterable[(Ref.SubmissionId, Configuration)],
  )(implicit
      materializer: Materializer
  ): (IndexConfigManagementService, WriteConfigService, () => Option[Configuration]) = {
    val currentOffset = new AtomicLong(startingOffset)
    val (configurationQueue, configurationSource) =
      Source.queue[(Long, SubmissionId, Configuration)](1).preMaterialize()
    submissions.foreach { case (submissionId, configuration) =>
      configurationQueue.offer((currentOffset.getAndIncrement(), submissionId, configuration))
    }
    val currentConfiguration =
      new AtomicReference[Option[(LedgerOffset.Absolute, Configuration)]](None)

    val indexService: IndexConfigManagementService = new IndexConfigManagementService {
      private val atLeastOneConfig = Promise[Unit]()
      private val source = configurationSource
        .map { case (offset, submissionId, configuration) =>
          val ledgerOffset =
            LedgerOffset.Absolute(Ref.LedgerString.assertFromString(offset.toString))
          currentConfiguration.set(Some(ledgerOffset -> configuration))
          atLeastOneConfig.trySuccess(())
          val entry = ConfigurationEntry.Accepted(submissionId, configuration)
          ledgerOffset -> entry
        }
        .preMaterialize()
      Await.result(atLeastOneConfig.future, ScalaDuration.Inf)

      override def lookupConfiguration()(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
        Future.successful(currentConfiguration.get())

      override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
          loggingContext: LoggingContextWithTrace
      ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] =
        source._2
    }
    val writeService = new WriteConfigService {
      override def submitConfiguration(
          maxRecordTime: Time.Timestamp,
          submissionId: SubmissionId,
          configuration: Configuration,
      )(implicit
          traceContext: TraceContext
      ): CompletionStage[SubmissionResult] = {
        configurationQueue.offer((currentOffset.getAndIncrement(), submissionId, configuration))
        completedFuture(state.SubmissionResult.Acknowledged)
      }
    }
    (indexService, writeService, () => currentConfiguration.get.map(_._2))
  }
}
