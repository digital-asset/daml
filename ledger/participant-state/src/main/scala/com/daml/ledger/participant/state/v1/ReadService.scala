// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import akka.NotUsed
import akka.stream.scaladsl.Source

trait ReadService {
  def stateUpdates(beginAfter: Option[UpdateId]): Source[(UpdateId, Update), NotUsed]
}
