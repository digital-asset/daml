// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import com.daml.ledger.participant.state.v1.LedgerId

import scala.concurrent.Future

trait IdentityService {
  def getLedgerId(): Future[LedgerId]
}
