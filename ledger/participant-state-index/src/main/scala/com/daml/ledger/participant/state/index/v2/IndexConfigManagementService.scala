// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.ledger.api.domain.{ConfigurationEntry, LedgerOffset}

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc]]
  *
  */
trait IndexConfigManagementService {

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to new configuration entries using [[configurationEntries]].
    */
  def lookupConfiguration(): Future[Option[(LedgerOffset.Absolute, Configuration)]]

  /** Retrieve configuration entries. */
  def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])
    : Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed]

}
