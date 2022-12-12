// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{LedgerServices, TestConfiguration}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

@deprecated("Ledger identity string is optional for all ledger API requests", since = "2.0.0")
final class LedgerIdentityClientTest extends AnyFlatSpec with Matchers with AuthMatchers {

  import LedgerIdentityClientTest._

  private val ledgerServices = new LedgerServices(ledgerId)

  behavior of "[6.1] LedgerIdentityClient.getLedgerIdentity"

  it should "return ledger-id when requested" in ledgerServices.withLedgerIdentityClient(
    alwaysSucceed
  ) { (binding, _) =>
    binding.getLedgerIdentity
      .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
      .blockingGet() shouldBe ledgerServices.ledgerId
  }

  it should "return ledger-id when requested with authorization" in ledgerServices
    .withLedgerIdentityClient(alwaysSucceed, mockedAuthService) { (binding, _) =>
      binding
        .getLedgerIdentity(publicToken)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet() shouldBe ledgerServices.ledgerId
    }

  it should "deny ledger-id queries with insufficient authorization" in ledgerServices
    .withLedgerIdentityClient(alwaysSucceed, mockedAuthService) { (binding, _) =>
      expectUnauthenticated {
        binding
          .getLedgerIdentity(emptyToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
      }
    }

  it should "timeout should work as expected across calls" in {
    ledgerServices.withLedgerIdentityClient(
      stepThrough(stuck, success),
      timeout = Optional.of(Duration.of(5, ChronoUnit.SECONDS)),
    ) { (client, _) =>
      withClue("The first command should be stuck") {
        expectDeadlineExceeded(
          client
            .getLedgerIdentity(emptyToken)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        )
      }

      withClue("The second command should go through") {
        val res = Option(
          client
            .getLedgerIdentity(emptyToken)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        )
        res should not be empty
      }
    }
  }
}

object LedgerIdentityClientTest {

  private val ledgerId = "ledger-identity-service-ledger"

  private val stuck = Future.never

  private val success = Future.successful(ledgerId)

  private val alwaysSucceed: () => Future[String] = () => success

  private def stepThrough[A](first: A, following: A*): () => A = {
    val it = Iterator.single(first) ++ Iterator(following: _*)
    () =>
      try {
        it.next()
      } catch {
        case e: NoSuchElementException =>
          throw new RuntimeException("LedgerIdentityClientImplTest.sequence exhausted", e)
      }
  }

}
