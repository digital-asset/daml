// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{LedgerServices, TestConfiguration}
import com.daml.ledger.api.v1.ledger_configuration_service.GetLedgerConfigurationResponse
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class LedgerConfigurationClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues {

  private val ledgerServices = new LedgerServices("ledger-configuration-service-ledger")

  behavior of "[5.1] LedgerConfigurationClientImpl.getLedgerConfiguration"

  it should "send the request to the Ledger" in {
    ledgerServices.withConfigurationClient(Seq(GetLedgerConfigurationResponse.defaultInstance)) {
      (client, _) =>
        // to test that we send a request to the Ledger, we check if there is a response
        client.getLedgerConfiguration
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingFirst()
    }
  }

  behavior of "[5.2] LedgerConfigurationClientImpl.getLedgerConfiguration"

  it should "send the request with the correct ledger ID" in {

    ledgerServices.withConfigurationClient(Seq(GetLedgerConfigurationResponse.defaultInstance)) {
      (client, service) =>
        client.getLedgerConfiguration
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingFirst()
        service.getLastRequest.value.ledgerId shouldEqual ledgerServices.ledgerId
    }
  }

  behavior of "Authorization"

  def toAuthenticatedServer(fn: LedgerConfigurationClient => Any): Any =
    ledgerServices.withConfigurationClient(
      Seq(GetLedgerConfigurationResponse.defaultInstance),
      mockedAuthService,
    ) { (client, _) =>
      fn(client)
    }

  it should "deny access without a token" in {
    expectUnauthenticated {
      toAuthenticatedServer(
        _.getLedgerConfiguration
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingFirst()
      )
    }
  }

  it should "deny access with insufficient authorization" in {
    expectUnauthenticated {
      toAuthenticatedServer(
        _.getLedgerConfiguration(emptyToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingFirst()
      )
    }
  }

  it should "allow access with sufficient authorization" in {
    toAuthenticatedServer(
      _.getLedgerConfiguration(publicToken)
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
    )
  }

}
