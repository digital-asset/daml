// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.digitalasset.canton.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc]]
  */
trait IndexConfigManagementService {

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to new configuration entries using [[configurationEntries]].
    */
  def lookupConfiguration()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(LedgerOffset.Absolute, Configuration)]]

  /** Retrieve configuration entries. */
  def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed]

}
