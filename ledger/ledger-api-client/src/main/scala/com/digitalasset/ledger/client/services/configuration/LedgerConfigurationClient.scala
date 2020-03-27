// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.configuration

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  LedgerConfiguration
}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.{
  LedgerConfigurationServiceStub
}
import com.digitalasset.ledger.client.LedgerClient
import scalaz.syntax.tag._

final class LedgerConfigurationClient(
    ledgerId: domain.LedgerId,
    service: LedgerConfigurationServiceStub)(implicit esf: ExecutionSequencerFactory) {

  def getLedgerConfiguration(token: Option[String] = None): Source[LedgerConfiguration, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetLedgerConfigurationRequest(ledgerId.unwrap),
        LedgerClient.stub(service, token).getLedgerConfiguration)
      .map(_.ledgerConfiguration.getOrElse(sys.error("No LedgerConfiguration in response.")))

}
