// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  PermissionErrors,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, TestingTopology, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
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

      new ReassigningParticipants(
        stakeholders = Set(alice, bob),
        sourceTopology = Source(snapshot),
        targetTopology = Target(snapshot),
      ).compute.futureValue shouldBe Set(p1, p2)
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

      new ReassigningParticipants(
        stakeholders = Set(alice, bob),
        sourceTopology = Source(source),
        targetTopology = Target(target), // p3 missing
      ).compute.futureValue shouldBe Set(p1, p2)

      new ReassigningParticipants(
        stakeholders = Set(alice, bob),
        sourceTopology = Source(source),
        targetTopology = Target(source), // p3 is there as well
      ).compute.futureValue shouldBe Set(p1, p2, p3)
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

      new ReassigningParticipants(
        stakeholders = Set(alice, bob),
        sourceTopology = Source(incomplete),
        targetTopology = Target(complete),
      ).compute.value.futureValue.left.value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the source domain: Set($bob)"
      )

      new ReassigningParticipants(
        stakeholders = Set(alice, bob),
        sourceTopology = Source(complete),
        targetTopology = Target(incomplete),
      ).compute.value.futureValue.left.value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the target domain: Set($bob)"
      )

      new ReassigningParticipants(
        stakeholders = Set(alice, bob),
        sourceTopology = Source(complete),
        targetTopology = Target(complete),
      ).compute.futureValue shouldBe Set(p1, p2)
    }

    "return all participants for a given party" in {
      val topology = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(alice -> ParticipantPermission.Confirmation),
        )
      )

      new ReassigningParticipants(
        stakeholders = Set(alice),
        sourceTopology = Source(topology),
        targetTopology = Target(topology),
      ).compute.futureValue shouldBe Set(p1, p2)
    }

    "only return participants with confirmation rights" in {
      val topology = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(alice -> ParticipantPermission.Observation),
        )
      )

      new ReassigningParticipants(
        stakeholders = Set(alice),
        sourceTopology = Source(topology),
        targetTopology = Target(topology),
      ).compute.futureValue shouldBe Set(p1)
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

      new ReassigningParticipants(
        stakeholders = Set(alice),
        sourceTopology = Source(source),
        targetTopology = Target(target),
      ).compute.value.futureValue.left.value shouldBe StakeholderHostingErrors(
        s"The following stakeholders are not hosted with confirmation rights on target domain: Set($alice)"
      )
    }

    "fail if one party has submission rights only on source domain" in {
      val source = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission),
          p2 -> Map(alice -> ParticipantPermission.Confirmation),
          p3 -> Map(alice -> ParticipantPermission.Confirmation),
        )
      )

      val targetCorrect = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Submission)
        )
      )

      val targetIncorrect1 = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Confirmation)
        )
      )

      val targetIncorrect2 = createTestingIdentityFactory(
        Map(
          p1 -> Map(alice -> ParticipantPermission.Confirmation),
          // alice not hosted on p3 with submission rights
          p3 -> Map(alice -> ParticipantPermission.Submission),
        )
      )

      new ReassigningParticipants(
        stakeholders = Set(alice),
        sourceTopology = Source(source),
        targetTopology = Target(targetCorrect),
      ).compute.futureValue shouldBe Set(p1)

      new ReassigningParticipants(
        stakeholders = Set(alice),
        sourceTopology = Source(source),
        targetTopology = Target(targetIncorrect1),
      ).compute.value.futureValue.left.value shouldBe PermissionErrors(
        s"For party $alice, no participant with submission permission on source domain has submission permission on target domain."
      )

      new ReassigningParticipants(
        stakeholders = Set(alice),
        sourceTopology = Source(source),
        targetTopology = Target(targetIncorrect2),
      ).compute.value.futureValue.left.value shouldBe PermissionErrors(
        s"For party $alice, no participant with submission permission on source domain has submission permission on target domain."
      )
    }
  }
}
