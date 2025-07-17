// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.config.GeneratorsConfig
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.{GeneratorsTopology, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsPartyReplication(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
    generatorsTopology: GeneratorsTopology,
) {
  import GeneratorsConfig.*
  import generatorsProtocol.*
  import generatorsTopology.*

  implicit val activeContractOldArb: Arbitrary[ActiveContractOld] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        serializableContract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
        reassignmentCounter <- Gen.chooseNum(0L, Long.MaxValue).map(ReassignmentCounter(_))
      } yield ActiveContractOld.create(
        synchronizerId,
        serializableContract,
        reassignmentCounter,
      )(protocolVersion)
    )

  implicit val partyReplicationSourceParticipantMessageArb
      : Arbitrary[PartyReplicationSourceParticipantMessage] =
    Arbitrary(
      for {
        acsBatch <- nonEmptyListGen[ActiveContractOld]
        message <- Gen
          .oneOf[PartyReplicationSourceParticipantMessage.DataOrStatus](
            PartyReplicationSourceParticipantMessage.AcsBatch(
              acsBatch
            ),
            Gen.const(PartyReplicationSourceParticipantMessage.SourceParticipantIsReady),
            Gen.const(PartyReplicationSourceParticipantMessage.EndOfACS),
          )
      } yield PartyReplicationSourceParticipantMessage.apply(
        message,
        protocolVersion,
      )
    )

  implicit val partyReplicationTargetParticipantMessageArb
      : Arbitrary[PartyReplicationTargetParticipantMessage] = Arbitrary(
    for {
      maxContractOrdinalInclusive <- nonNegativeIntArb.arbitrary
      instruction =
        PartyReplicationTargetParticipantMessage.SendAcsSnapshotUpTo(
          maxContractOrdinalInclusive
        )
    } yield PartyReplicationTargetParticipantMessage.apply(
      instruction,
      protocolVersion,
    )
  )
}
