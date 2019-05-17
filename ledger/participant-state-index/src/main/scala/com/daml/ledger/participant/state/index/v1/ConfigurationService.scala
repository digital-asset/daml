// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import com.daml.ledger.participant.state.v1.Configuration

import scala.concurrent.Future

trait ConfigurationService {
  def getLedgerConfiguration(): Future[Configuration]
}
