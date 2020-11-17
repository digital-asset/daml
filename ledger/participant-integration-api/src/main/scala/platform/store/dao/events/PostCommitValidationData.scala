// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.api.domain.PartyDetails

import scala.util.Try

private[events] trait PostCommitValidationData {

  def lookupContractKeyGlobally(key: Key)(implicit connection: Connection): Option[ContractId]

  def lookupMaximumLedgerTime(ids: Set[ContractId])(
      implicit connection: Connection): Try[Option[Instant]]

  def lookupParties(parties: Seq[Party])(implicit connection: Connection): List[PartyDetails]
}
