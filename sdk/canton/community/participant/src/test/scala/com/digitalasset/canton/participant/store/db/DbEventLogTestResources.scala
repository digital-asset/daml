// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.participant.store.EventLogId
import com.digitalasset.canton.participant.store.EventLogId.{
  DomainEventLogId,
  ParticipantEventLogId,
}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.DomainId

/** Resources shared between tests acceding DbEventLogs.
  * Make sure that the resources here do not overlap for different tests.
  */
private[store] object DbEventLogTestResources {

  lazy val dbSingleDimensionEventLogEventLogId: EventLogId =
    DomainEventLogId(
      IndexedDomain.tryCreate(
        DomainId.tryFromString("TestDomain::DbSingleDimensionEventLogTest"),
        1,
      )
    )

  lazy val dbParticipantEventLogTestParticipantEventLogId: ParticipantEventLogId =
    ParticipantEventLogId.tryCreate(-1)

  lazy val dbMultiDomainEventLogTestIndexedStringStore: InMemoryIndexedStringStore =
    new InMemoryIndexedStringStore(10, 19)

  lazy val dbMultiDomainEventLogTestParticipantEventLogId: ParticipantEventLogId =
    ParticipantEventLogId.tryCreate(-2)

  lazy val dbCausalityStoresTestIndexedStringStore: InMemoryIndexedStringStore =
    new InMemoryIndexedStringStore(20, 29)
}
