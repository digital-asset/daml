// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.ParticipantSettingsStoreTest

class ParticipantSettingsStoreTestInMemory extends ParticipantSettingsStoreTest {

  "InMemoryParticipantResourceManagementStore" must {
    behave like participantSettingsStore(() => new InMemoryParticipantSettingsStore(loggerFactory))
  }
}
