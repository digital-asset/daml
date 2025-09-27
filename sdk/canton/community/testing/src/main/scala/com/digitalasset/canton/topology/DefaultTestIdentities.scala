// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters

object DefaultTestIdentities {
  import BaseTest.*

  private def createParticipantAndParty(counter: Int): (ParticipantId, PartyId) = {
    val namespace = Namespace(Fingerprint.tryFromString(s"participant$counter-identity"))
    val id = ParticipantId(UniqueIdentifier.tryCreate(s"participant$counter", namespace))
    val party = PartyId(UniqueIdentifier.tryCreate(s"party$counter", namespace))
    (id, party)
  }

  val namespace: Namespace = Namespace(Fingerprint.tryFromString("default"))
  val uid: UniqueIdentifier = UniqueIdentifier.tryCreate("da", namespace)
  val synchronizerId: SynchronizerId = SynchronizerId(uid)
  val physicalSynchronizerId: PhysicalSynchronizerId = synchronizerId.toPhysical

  val daSequencerId: SequencerId = SequencerId(uid)
  val daMediator: MediatorId = MediatorId(uid)

  val sequencerId: SequencerId = SequencerId(UniqueIdentifier.tryCreate("sequencer", namespace))
  val mediatorId: MediatorId = MediatorId(UniqueIdentifier.tryCreate("mediator", namespace))

  val (participant1, party1) = createParticipantAndParty(1)
  val (participant2, party2) = createParticipantAndParty(2)
  val (participant3, party3) = createParticipantAndParty(3)

  val defaultDynamicSynchronizerParameters: DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.initialValues(
      BaseTest.testedProtocolVersion
    )
}
