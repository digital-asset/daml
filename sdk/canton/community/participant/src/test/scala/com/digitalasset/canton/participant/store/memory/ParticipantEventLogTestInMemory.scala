// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.{ParticipantEventLog, ParticipantEventLogTest}

class ParticipantEventLogTestInMemory extends ParticipantEventLogTest {
  override def newStore: ParticipantEventLog = new InMemoryParticipantEventLog(id, loggerFactory)
}
