// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

final class TimeClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("time-service-ledger")
  implicit val ec: ExecutionContext = ledgerServices.executionContext

  behavior of "[9.1] TimeClientImpl.setTime"

  it should "send requests with the correct ledger ID, current time and new time" in {
    val (timeService, timeServiceImpl) = TimeServiceImpl.createWithRef(Seq.empty, authorizer)
    ledgerServices.withTimeClient(Seq(timeService)) { timeClient =>
      val currentLedgerTimeSeconds = 1L
      val currentLedgerTimeNanos = 2L
      val currentTime = Instant.ofEpochSecond(currentLedgerTimeSeconds, currentLedgerTimeNanos)
      val newLedgerTimeSeconds = 3L
      val newLedgerTimeNanos = 4L
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
    ledgerServices.withTimeClient(
      Seq(TimeServiceImpl.createWithRef(Seq(getTimeResponse), authorizer)._1)
    ) { timeClient =>
      val response = timeClient.getTime
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      response.getEpochSecond shouldEqual getTimeResponse.currentTime.value.seconds
      response.getNano shouldEqual getTimeResponse.currentTime.value.nanos
    }
  }

  behavior of "[9.3] TimeClientImpl.getTime"

  it should "send requests with the correct ledger ID" in {
    val getTimeResponse =
      genGetTimeResponse // we use the first element to block on the first element
    val (service, impl) = TimeServiceImpl.createWithRef(Seq(getTimeResponse), authorizer)
    ledgerServices.withTimeClient(Seq(service)) { timeClient =>
      val _ = timeClient
        .getTime()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      impl.getLastGetTimeRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "[9.4] TimeClientImpl.setTime"

  it should "return an error without sending a request when the time to set if bigger than the current time" in {
    ledgerServices.withTimeClient(Seq(TimeServiceImpl.createWithRef(Seq.empty, authorizer)._1)) {
      timeClient =>
        val currentTime = Instant.ofEpochSecond(1L, 2L)
        intercept[RuntimeException](
          timeClient
            .setTime(currentTime, currentTime)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        )
    }
  }

  behavior of "Authorization"

  def toAuthenticatedServer(fn: TimeClient => Any): Any =
    ledgerServices.withTimeClient(
      Seq(TimeServiceImpl.createWithRef(Seq(genGetTimeResponse), authorizer)._1),
      mockedAuthService,
    )(fn)

  it should "deny access without a token" in {
    withClue("getTime") {
      expectUnauthenticated {
        toAuthenticatedServer {
          _.getTime().blockingFirst()
        }
      }
    }
    withClue("setTime") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          val t = Instant.now()
          client.setTime(t, t.plusSeconds(1)).blockingGet()
        }
      }
    }
  }

  it should "deny access with insufficient authorization" in {
    withClue("getTime") {
      expectUnauthenticated {
        toAuthenticatedServer {
          _.getTime(emptyToken).blockingFirst()
        }
      }
    }
    withClue("setTime") {
      expectPermissionDenied {
        toAuthenticatedServer { client =>
          val t = Instant.now()
          client.setTime(t, t.plusSeconds(1), somePartyReadWriteToken).blockingGet()
        }
      }
    }
  }

  it should "allow calls with sufficient authorization" in {
    toAuthenticatedServer {
      withClue("getTime") {
        _.getTime(publicToken).blockingFirst()
      }
    }
    toAuthenticatedServer { client =>
      withClue("setTime") {
        val t = Instant.now()
        client.setTime(t, t.plusSeconds(1), adminToken).blockingGet()
      }
    }
  }

}
