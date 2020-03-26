// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{LedgerServices, TestConfiguration}
import org.scalatest.{FlatSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class LedgerIdentityClientTest extends FlatSpec with Matchers with AuthMatchers {

  val ledgerServices = new LedgerServices("ledger-identity-service-ledger")

  behavior of "[6.1] LedgerIdentityClient.getLedgerIdentity"

  it should "return ledger-id when requested" in ledgerServices.withLedgerIdentityClient() {
    (binding, _) =>
      binding.getLedgerIdentity
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet() shouldBe ledgerServices.ledgerId
  }

  it should "return ledger-id when requested with authorization" in ledgerServices
    .withLedgerIdentityClient(mockedAuthService) { (binding, _) =>
      binding
        .getLedgerIdentity(publicToken)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet() shouldBe ledgerServices.ledgerId
    }

  it should "deny ledger-id queries with insufficient authorization" in ledgerServices
    .withLedgerIdentityClient(mockedAuthService) { (binding, _) =>
      expectUnauthenticated {
        binding
          .getLedgerIdentity(emptyToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
      }
    }

}
