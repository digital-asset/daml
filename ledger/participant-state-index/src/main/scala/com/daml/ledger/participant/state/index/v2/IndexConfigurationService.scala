// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService]]
  **/
trait IndexConfigurationService {
  def getLedgerConfiguration(): Source[LedgerConfiguration, NotUsed]
}
