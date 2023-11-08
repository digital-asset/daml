// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.digitalasset.canton.logging.LoggingContextWithTrace

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService]]
  */
trait IndexConfigurationService {
  def getLedgerConfiguration()(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[LedgerConfiguration, NotUsed]
}
