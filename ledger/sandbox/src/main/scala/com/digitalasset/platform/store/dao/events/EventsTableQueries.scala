// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.ledger.participant.state.v1.Offset

object EventsTableQueries {

  def previousOffsetWhereClauseValues(
      between: (Offset, Offset),
      lastEventNodeIndexFromPreviousPage: Option[Int]
  ): (Offset, Int) = previousOffsetWhereClauseValues(between._1, lastEventNodeIndexFromPreviousPage)

  def previousOffsetWhereClauseValues(
      lastOffsetFromPreviousPage: Offset,
      lastEventNodeIndexFromPreviousPage: Option[Int]
  ): (Offset, Int) =
    lastEventNodeIndexFromPreviousPage
      .map(x => (lastOffsetFromPreviousPage, x))
      .getOrElse((Offset.begin, Integer.MAX_VALUE)) // nonsense
}
