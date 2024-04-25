// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration

object DefaultTestIdentities {

  private def createParticipantAndParty(counter: Int): (ParticipantId, PartyId) = {
    val namespace = Namespace(Fingerprint.tryCreate(s"participant$counter-identity"))
    val id = ParticipantId(Identifier.tryCreate(s"participant$counter"), namespace)
    val party = PartyId(UniqueIdentifier(Identifier.tryCreate(s"party$counter"), namespace))
    (id, party)
  }

  val namespace = Namespace(Fingerprint.tryCreate("default"))
  val uid = UniqueIdentifier(Identifier.tryCreate("da"), namespace)
  val domainId = DomainId(uid)

  val sequencerId = SequencerId(uid)
  val mediator = MediatorId(uid)

  val sequencerIdX = SequencerId(UniqueIdentifier(Identifier.tryCreate("sequencer"), namespace))
  val mediatorIdX = MediatorId(UniqueIdentifier(Identifier.tryCreate("mediator"), namespace))

  val (participant1, party1) = createParticipantAndParty(1)
  val (participant2, party2) = createParticipantAndParty(2)
  val (participant3, party3) = createParticipantAndParty(3)

  val defaultDynamicDomainParameters =
    DynamicDomainParameters.initialValues(
      NonNegativeFiniteDuration.Zero,
      BaseTest.testedProtocolVersion,
    )
}
