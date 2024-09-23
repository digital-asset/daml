// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.StakeholderHostingErrors
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, TestingTopology, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

class ReassigningParticipantsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private def createTestingIdentityFactory(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]]
  ): TopologySnapshot =
    TestingTopology()
      .withReversedTopology(topology)
      .build(loggerFactory)
      .topologySnapshot()

  private lazy val alice: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("alice::party")
  ).toLf
  private lazy val bob: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("bob::party")
  ).toLf
  private lazy val charlie: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("charlie::party")
  ).toLf

  private lazy val p1 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("p1::participant1")
  )
  private lazy val p2 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("p2::participant2")
  )
  private lazy val p3 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("p3::participant3 ")
  )

  "ReassigningParticipants" should {
    "compute reassigning participants (homogeneous topology)" in {
      val snapshot = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(bob -> ParticipantPermission.Submission),
          p3 -> Map(charlie -> ParticipantPermission.Submission),
        )
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice, bob),
          sourceTopology = snapshot,
          targetTopology = snapshot,
        )
        .futureValue shouldBe Set(p1, p2)
    }

    "not return participants connected to a single domain" in {
      val source = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(bob -> ParticipantPermission.Submission),
          p3 -> Map(alice -> Submission),
        )
      )

      val target = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(bob -> ParticipantPermission.Submission),
        )
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice, bob),
          sourceTopology = source,
          targetTopology = target, // p3 missing
        )
        .futureValue shouldBe Set(p1, p2)

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice, bob),
          sourceTopology = source,
          targetTopology = source, // p3 is there as well
        )
        .futureValue shouldBe Set(p1, p2, p3)
    }

    "fail if one stakeholder is unknown in the topology state" in {
      val incomplete = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission)
        )
      )

      val complete = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(bob -> ParticipantPermission.Submission),
        )
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice, bob),
          sourceTopology = incomplete,
          targetTopology = complete,
        )
        .value
        .futureValue
        .left
        .value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the source domain: Set($bob)"
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice, bob),
          sourceTopology = complete,
          targetTopology = incomplete,
        )
        .value
        .futureValue
        .left
        .value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the target domain: Set($bob)"
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice, bob),
          sourceTopology = complete,
          targetTopology = complete,
        )
        .futureValue shouldBe Set(p1, p2)
    }

    "return all participants for a given party" in {
      val topology = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(alice -> ParticipantPermission.Confirmation),
        )
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice),
          sourceTopology = topology,
          targetTopology = topology,
        )
        .futureValue shouldBe Set(p1, p2)
    }

    "only return participants with confirmation rights" in {
      val topology = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(alice -> ParticipantPermission.Observation),
        )
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice),
          sourceTopology = topology,
          targetTopology = topology,
        )
        .futureValue shouldBe Set(p1)
    }

    "fail if one party is not hosted with confirmation rights on a domain" in {
      val source = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Confirmation)
        )
      )

      val target = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Observation)
        )
      )

      ReassigningParticipants
        .compute(
          stakeholders = Set(alice),
          sourceTopology = source,
          targetTopology = target,
        )
        .value
        .futureValue
        .left
        .value shouldBe StakeholderHostingErrors(
        s"The following stakeholders are not hosted with confirmation rights on target domain: Set($alice)"
      )
    }
  }
}
