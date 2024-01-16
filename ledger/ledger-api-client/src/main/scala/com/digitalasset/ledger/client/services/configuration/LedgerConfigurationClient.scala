// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.configuration

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  LedgerConfiguration,
}
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationServiceStub
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

final class LedgerConfigurationClient(
    ledgerId: domain.LedgerId,
    service: LedgerConfigurationServiceStub,
)(implicit esf: ExecutionSequencerFactory) {

  def getLedgerConfiguration(token: Option[String] = None): Source[LedgerConfiguration, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetLedgerConfigurationRequest(ledgerId.unwrap),
        LedgerClient.stub(service, token).getLedgerConfiguration,
      )
      .map(_.ledgerConfiguration.getOrElse(sys.error("No LedgerConfiguration in response.")))

}
