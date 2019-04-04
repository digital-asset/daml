// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.daml.ledger.testkit.services.TimeServiceImpl
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.concurrent.ExecutionContext

class TimeClientImplTest extends FlatSpec with Matchers with OptionValues with DataLayerHelpers {

  val ledgerServices = new LedgerServices("time-service-ledger")
  implicit val ec: ExecutionContext = ledgerServices.executionContext

  behavior of "[9.1] TimeClientImpl.setTime"

  it should "send requests with the correct ledger ID, current time and new time" in {
    val (timeService, timeServiceImpl) = TimeServiceImpl.createWithRef()
    ledgerServices.withTimeClient(timeService) { timeClient =>
      val currentLedgerTimeSeconds = 1l
      val currentLedgerTimeNanos = 2l
      val currentTime = Instant.ofEpochSecond(currentLedgerTimeSeconds, currentLedgerTimeNanos)
      val newLedgerTimeSeconds = 3l
      val newLedgerTimeNanos = 4l
      val newTime =
        Instant.ofEpochSecond(newLedgerTimeSeconds, newLedgerTimeNanos)
      val _ = timeClient
        .setTime(currentTime, newTime)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      timeServiceImpl.getLastSetTimeRequest.value.getCurrentTime.seconds shouldBe currentLedgerTimeSeconds
      timeServiceImpl.getLastSetTimeRequest.value.getCurrentTime.nanos shouldBe currentLedgerTimeNanos
      timeServiceImpl.getLastSetTimeRequest.value.getNewTime.seconds shouldBe newLedgerTimeSeconds
      timeServiceImpl.getLastSetTimeRequest.value.getNewTime.nanos shouldBe newLedgerTimeNanos
      timeServiceImpl.getLastSetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "[9.2] TimeClientImpl.getTime"

  it should "return the responses received" in {
    val getTimeResponse = genGetTimeResponse
    ledgerServices.withTimeClient(TimeServiceImpl(getTimeResponse)) { timeClient =>
      val response = timeClient.getTime
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      response.getEpochSecond shouldEqual getTimeResponse.currentTime.value.seconds
      response.getNano shouldEqual getTimeResponse.currentTime.value.nanos
    }
  }

  behavior of "[9.3] TimeClientImpl.getTime"

  it should "send requests with the correct ledger ID" in {
    val getTimeResponse = genGetTimeResponse // we use the first element to block on the first element
    val (service, impl) = TimeServiceImpl.createWithRef(getTimeResponse)
    ledgerServices.withTimeClient(service) { timeClient =>
      val _ = timeClient
        .getTime()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      impl.getLastGetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "[9.4] TimeClientImpl.setTime"

  it should "return an error without sending a request when the time to set if bigger than the current time" in {
    ledgerServices.withTimeClient(TimeServiceImpl()) { timeClient =>
      val currentTime = Instant.ofEpochSecond(1l, 2l)
      intercept[RuntimeException](
        timeClient
          .setTime(currentTime, currentTime)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
      )
    }
  }
}
