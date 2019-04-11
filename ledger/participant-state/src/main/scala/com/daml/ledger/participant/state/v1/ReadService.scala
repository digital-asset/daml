// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import akka.NotUsed
import akka.stream.scaladsl.Source

trait ReadService {

  /** Get the stream of state updates starting from the beginning of the
    * given [[Offset]]
    *
    * Correct implementations of this method need to satisfy the properties
    * listed below. These properties fall into two categories:
    *
    * 1. properties about the sequence of [[(UpdateId, Update)]] tuples
    *    in a stream read from the beginning, and
    * 2. properties relating the streams obtained from two separate alls
    *   to [[ReadService.stateUpdates]].
    *
    * We explain them in turn.
    *
    * - *strictly increasing [[Offset]]s*:
    *   for any two consecutive tuples `(i1, u1)` and `(i2,u2)` in the
    *   stream `i1` is strictly smaller than `i2` when lexicographically
    *   comparing the [[Offset]]s.
    *
    * - *initialize before transaction acceptance*: before any
    *   [[Update.TransactionAccepted]], there are a [[Update.ConfigurationChanged]] update
    *   and [[Update.PublicPackageUploaded]] updates for all packages referenced by
    *   the [[Update.TransactionAccepted]].
    *
    * TODO (SM): finish documentation
    * https://github.com/digital-asset/daml/issues/138
    *
    */
  def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed]
}
