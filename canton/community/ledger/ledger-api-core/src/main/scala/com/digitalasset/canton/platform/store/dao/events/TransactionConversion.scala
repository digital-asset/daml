// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v1.event.Event
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps

private[platform] object TransactionConversion {

  private def permanent(events: Seq[Event]): Set[String] = {
    events.foldLeft(Set.empty[String]) { (contractIds, event) =>
      if (event.isCreated || !contractIds.contains(event.contractId)) {
        contractIds + event.contractId
      } else {
        contractIds - event.contractId
      }
    }
  }

  // `events` must be in creation order
  private[platform] def removeTransient(events: Seq[Event]): Seq[Event] = {
    val toKeep = permanent(events)
    events.filter(event => toKeep(event.contractId))
  }
}
