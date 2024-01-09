// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.topology.transaction.ParticipantPermissionX.{
  Confirmation,
  Observation,
  Submission,
}
import org.scalatest.wordspec.AnyWordSpec

class PartyToParticipantComputationsTest extends AnyWordSpec with BaseTest {

  private lazy val computations = new PartyToParticipantComputations(loggerFactory)

  private def participantIdFor(idx: Int) = {
    val namespace = Namespace(Fingerprint.tryCreate(s"participant$idx-identity"))
    ParticipantId(Identifier.tryCreate(s"participant$idx"), namespace)
  }

  private val p1 = participantIdFor(1)
  private val p2 = participantIdFor(2)
  private val p3 = participantIdFor(3)

  "PartyToParticipantComputations" should {
    "return an error when adds and remove overlap" in {
      computations
        .computeNewPermissions(
          Map(p1 -> Submission),
          adds = List((p2, Submission)),
          removes = List(p2),
        )
        .left
        .value should include(
        "Permissions for the following participant were found in adds and removes:"
      )
    }

    "return an error when trying to remove a participant which is not permissioned" in {
      computations
        .computeNewPermissions(
          Map(p1 -> Submission),
          removes = List(p2),
        )
        .left
        .value shouldBe s"Cannot remove permission for participants that are not permissioned: ${Set(p2)}"

      computations
        .computeNewPermissions(
          Map(p1 -> Submission),
          removes = List(p1),
        )
        .value shouldBe empty
    }

    "allow to add and remove permissions" in {
      computations
        .computeNewPermissions(
          Map(p1 -> Submission, p2 -> Observation),
          adds = List((p3, Confirmation)),
          removes = List(p1),
        )
        .value shouldBe Map(p2 -> Observation, p3 -> Confirmation)
    }

    "update existing added permissions" in {
      val updated = Map(p1 -> Confirmation)
      computations
        .computeNewPermissions(Map(p1 -> Submission), adds = List((p1, Confirmation)))
        .value shouldBe updated
    }
  }
}
