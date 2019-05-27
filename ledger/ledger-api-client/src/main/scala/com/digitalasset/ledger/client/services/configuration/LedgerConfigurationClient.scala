// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.configuration

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{GetLedgerConfigurationRequest, LedgerConfiguration}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService

import scalaz.syntax.tag._

final class LedgerConfigurationClient(
    ledgerId: domain.LedgerId,
    ledgerConfigurationService: LedgerConfigurationService)(
    implicit esf: ExecutionSequencerFactory) {

  def getLedgerConfiguration: Source[LedgerConfiguration, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetLedgerConfigurationRequest(ledgerId.unwrap),
        ledgerConfigurationService.getLedgerConfiguration)
      .map(_.ledgerConfiguration.getOrElse(sys.error("No LedgerConfiguration in response.")))

}
