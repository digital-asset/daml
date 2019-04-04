// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.GetLedgerConfigurationResponse
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class LedgerConfigurationClientImplTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("ledger-configuration-service-ledger")

  behavior of "[5.1] LedgerConfigurationClientImpl.getLedgerConfiguration"

  it should "send the request to the Ledger" in {
    ledgerServices.withConfigurationClient(Seq(GetLedgerConfigurationResponse.defaultInstance)) {
      (client, _) =>
        // to test that we send a request to the Ledger, we check if there is a response
        client.getLedgerConfiguration
          .take(1)
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
}
