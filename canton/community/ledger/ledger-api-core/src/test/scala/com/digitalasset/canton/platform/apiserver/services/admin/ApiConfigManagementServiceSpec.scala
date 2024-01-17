// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ErrorsAssertions
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.config_management_service.{GetTimeModelRequest, TimeModel}
import com.daml.lf.data.Ref
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.admin.ApiConfigManagementServiceSpec.*
import com.digitalasset.canton.tracing.TestTelemetrySetup
import com.google.protobuf.duration.Duration as DurationProto
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterEach, Inside}

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
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

      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        new FakeCurrentIndexConfigManagementService(
          LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")),
          Configuration(aConfigurationGeneration, indexedTimeModel, Duration.ZERO),
        ),
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      apiConfigManagementService
        .getTimeModel(GetTimeModelRequest.defaultInstance)
        .map { response =>
          response.timeModel should be(Some(expectedTimeModel))
          succeed
        }
    }

    "return a `NOT_FOUND` error if a time model is not found" in {
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        EmptyIndexConfigManagementService,
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
  }
}

object ApiConfigManagementServiceSpec {

  private val aConfigurationGeneration = 0L

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
  }
}
