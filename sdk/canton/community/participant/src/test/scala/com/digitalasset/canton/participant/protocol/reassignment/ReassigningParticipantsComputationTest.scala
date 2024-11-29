// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.StakeholderHostingErrors
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, TestingTopology, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

class ReassigningParticipantsComputationTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private def createTestingIdentityFactory(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]]
  ): TopologySnapshot =
    TestingTopology()
      .withReversedTopology(topology)
      .build(loggerFactory)
      .topologySnapshot()

  private def createTestingWithThreshold(
      topology: Map[LfPartyId, (PositiveInt, Seq[(ParticipantId, ParticipantPermission)])]
  ): TopologySnapshot =
    TestingTopology()
      .withThreshold(topology)
      .build(loggerFactory)
      .topologySnapshot()

  private lazy val signatory: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("signatory::party")
  ).toLf
  private lazy val observer: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("observer::party")
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
    UniqueIdentifier.tryFromProtoPrimitive("p3::participant3")
  )
  private lazy val p4 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("p4::participant4")
  )

  "ReassigningParticipants" should {
    "compute reassigning participants (homogeneous topology)" in {
      val snapshot = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission),
          p2 -> Map(observer -> ParticipantPermission.Submission),
          p3 -> Map(charlie -> ParticipantPermission.Submission),
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatoriesAndObservers(Set(signatory), Set(observer)),
        sourceTopology = Source(snapshot),
        targetTopology = Target(snapshot),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)
    }

    "not return participants connected to a single domain" in {
      val stakeholders = Stakeholders.withSignatoriesAndObservers(Set(signatory), Set(observer))

      val source = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission),
          p2 -> Map(observer -> ParticipantPermission.Submission),
          p3 -> Map(signatory -> Submission),
        )
      )

      val target = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission),
          p2 -> Map(observer -> ParticipantPermission.Submission),
          p4 -> Map(observer -> ParticipantPermission.Submission),
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(source), // p4 missing
        targetTopology = Target(target), // p3 missing
      ).compute.futureValueUS.value shouldBe Set(p1, p2)

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(source),
        targetTopology = Target(source), // p3 is there as well
      ).compute.futureValueUS.value shouldBe Set(p1, p2, p3)

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(target), // p4 is there as well
        targetTopology = Target(target),
      ).compute.futureValueUS.value shouldBe Set(p1, p2, p4)
    }

    "fail if one signatory is unknown in the topology state" in {
      val stakeholders = Stakeholders.withSignatoriesAndObservers(Set(signatory), Set(observer))

      val incomplete = createTestingIdentityFactory(
        Map(
          p1 -> Map(observer -> ParticipantPermission.Submission)
        )
      )

      val complete = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission),
          p2 -> Map(observer -> ParticipantPermission.Submission),
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(incomplete),
        targetTopology = Target(complete),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the source domain: Set($signatory)"
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(complete),
        targetTopology = Target(incomplete),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the target domain: Set($signatory)"
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(complete),
        targetTopology = Target(complete),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)
    }

    "fail if one observer is unknown in the topology state" in {
      val stakeholders = Stakeholders.withSignatoriesAndObservers(Set(signatory), Set(observer))

      val incomplete = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission)
        )
      )

      val complete = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission),
          p2 -> Map(observer -> ParticipantPermission.Submission),
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(incomplete),
        targetTopology = Target(complete),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the source domain: Set($observer)"
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(complete),
        targetTopology = Target(incomplete),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"The following parties are not active on the target domain: Set($observer)"
      )

      new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology = Source(complete),
        targetTopology = Target(complete),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)
    }

    "return all participants for a given party" in {
      val topology = createTestingIdentityFactory(
        Map(
          p1 -> Map(signatory -> ParticipantPermission.Submission),
          p2 -> Map(signatory -> ParticipantPermission.Confirmation),
          p3 -> Map(observer -> ParticipantPermission.Submission),
          p4 -> Map(observer -> ParticipantPermission.Confirmation),
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatoriesAndObservers(Set(signatory), Set(observer)),
        sourceTopology = Source(topology),
        targetTopology = Target(topology),
      ).compute.futureValueUS.value shouldBe Set(p1, p2, p3, p4)

    }

    "return participants with observations rights" in {
      val topology = createTestingIdentityFactory(
        Map(
          p1 -> Map(observer -> ParticipantPermission.Confirmation),
          p2 -> Map(observer -> ParticipantPermission.Observation),
          p3 -> Map(signatory -> ParticipantPermission.Confirmation),
          p4 -> Map(signatory -> ParticipantPermission.Observation),
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatoriesAndObservers(Set(signatory), Set(observer)),
        sourceTopology = Source(topology),
        targetTopology = Target(topology),
      ).compute.futureValueUS.value shouldBe Set(p1, p2, p3, p4)
    }

    "fail if there are not enough signatory reassigning participants" in {
      // ti_c_x_o: threshold is i, p1 hosts signatory with Confirmation, p2 does not host signatory, p3 hosts signatory with Observing

      val t1_c_x_x = createTestingWithThreshold(
        Map(signatory -> (PositiveInt.one, Seq((p1, Confirmation))))
      )
      val t2_c_c_x = createTestingWithThreshold(
        Map(
          signatory -> (PositiveInt.two, Seq((p1, Confirmation), (p2, Confirmation)))
        )
      )
      val t1_c_o_x = createTestingWithThreshold(
        Map(
          signatory -> (PositiveInt.one, Seq((p1, Confirmation), (p2, Observation)))
        )
      )
      val t2_c_o_c = createTestingWithThreshold(
        Map(
          signatory -> (PositiveInt.two, Seq(
            (p1, Confirmation),
            (p2, Observation),
            (p3, Confirmation),
          ))
        )
      )
      val t2_c_c_c = createTestingWithThreshold(
        Map(
          signatory -> (PositiveInt.two, Seq(
            (p1, Confirmation),
            (p2, Confirmation),
            (p3, Confirmation),
          ))
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t1_c_x_x),
        targetTopology = Target(t1_c_x_x),
      ).compute.futureValueUS.value shouldBe Set(p1)

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_c_x),
        targetTopology = Target(t2_c_c_c),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)
      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_c_c),
        targetTopology = Target(t2_c_c_x),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_c_x),
        targetTopology = Target(t1_c_o_x),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)
      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t1_c_o_x),
        targetTopology = Target(t2_c_c_x),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_o_c),
        targetTopology = Target(t2_c_o_c),
      ).compute.futureValueUS.value shouldBe Set(p1, p2, p3)
      // Errors
      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t1_c_x_x),
        targetTopology = Target(t2_c_c_x),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"Signatory $signatory requires at least 2 signatory reassigning participants on target domain, but only 1 are available"
      )
      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_c_x),
        targetTopology = Target(t1_c_x_x),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"Signatory $signatory requires at least 2 signatory reassigning participants on source domain, but only 1 are available"
      )

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t1_c_x_x),
        targetTopology = Target(t2_c_o_c),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"Signatory $signatory requires at least 2 signatory reassigning participants on target domain, but only 1 are available"
      )
      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_o_c),
        targetTopology = Target(t1_c_x_x),
      ).compute.value.futureValueUS.left.value shouldBe StakeholderHostingErrors(
        s"Signatory $signatory requires at least 2 signatory reassigning participants on source domain, but only 1 are available"
      )
    }

    "not require confirmation on both domains" in {
      val t1_c_o_x = createTestingWithThreshold(
        Map(signatory -> (PositiveInt.one, Seq((p1, Confirmation), (p2, Observation))))
      )
      val t2_c_c_c = createTestingWithThreshold(
        Map(
          signatory -> (PositiveInt.two, Seq(
            (p1, Confirmation),
            (p2, Confirmation),
            (p3, Confirmation),
          ))
        )
      )

      new ReassigningParticipantsComputation(
        stakeholders = Stakeholders.withSignatories(Set(signatory)),
        sourceTopology = Source(t2_c_c_c),
        targetTopology = Target(t1_c_o_x),
      ).compute.futureValueUS.value shouldBe Set(p1, p2)
    }
  }
}
