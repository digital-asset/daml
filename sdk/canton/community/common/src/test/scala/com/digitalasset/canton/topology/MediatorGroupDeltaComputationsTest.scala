// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.topology.transaction.MediatorDomainStateX
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@nowarn("msg=match may not be exhaustive")
class MediatorGroupDeltaComputationsTest extends AnyWordSpec with BaseTest {
  private def mediatorIdFor(idx: Int) = {
    val namespace = Namespace(Fingerprint.tryCreate(s"m${idx}"))
    MediatorId(Identifier.tryCreate(s"mediator$idx"), namespace)
  }

  private lazy val Seq(m1, m2, m3, m4) = (1 to 4).map(mediatorIdFor)

  def range(from: Int, to: Int): Seq[MediatorId] = from to to map mediatorIdFor

  def mds(active: Seq[MediatorId], observers: Seq[MediatorId]): Option[MediatorDomainStateX] =
    Some(
      MediatorDomainStateX
        .create(
          DefaultTestIdentities.domainId,
          NonNegativeInt.zero,
          PositiveInt.one,
          active,
          observers,
        )
        .value
    )

  "MediatorGroupDeltaComputations.verifyProposalConsistency" should {
    "succeed on non-overlapping mediatorIds" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = range(1, 2),
          removes = range(3, 4),
          observerAdds = range(5, 6),
          observerRemoves = range(7, 8),
          updateThreshold = None,
        )
        .value shouldBe ()
    }

    "succeed when making active mediators observers and vice versa" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = range(1, 2),
          removes = range(3, 4),
          observerAdds = range(3, 4),
          observerRemoves = range(1, 2),
          updateThreshold = None,
        )
        .value shouldBe ()
    }

    "complain about empty changes" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = Nil,
          removes = Nil,
          observerAdds = Nil,
          observerRemoves = Nil,
          updateThreshold = None,
        )
        .leftOrFail("bad proposal") shouldBe "no mediator group changes proposed"
    }

    "complain about overlapping adds and removes" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = range(1, 2),
          removes = range(2, 3),
          observerAdds = Nil,
          observerRemoves = Nil,
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "the same mediators MED::mediator2::m2 cannot be added and removed as active in the same proposal"
    }

    "complain about overlapping adds and observer adds" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = range(1, 2),
          removes = Nil,
          observerAdds = range(2, 3),
          observerRemoves = Nil,
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "the same mediators MED::mediator2::m2 cannot be added as active and observer in the same proposal"
    }

    "complain about overlapping observer adds and observer removes" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = Nil,
          removes = Nil,
          observerAdds = range(1, 2),
          observerRemoves = range(2, 3),
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "the same mediators MED::mediator2::m2 cannot be added and removed as observer in the same proposal"
    }

    "complain about overlapping removes and observer removes" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = Nil,
          removes = range(1, 2),
          observerAdds = Nil,
          observerRemoves = range(2, 3),
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "the same mediators MED::mediator2::m2 cannot be removed as active and observer in the same proposal"
    }

    "complain about multiple overlapping changes" in {
      MediatorGroupDeltaComputations
        .verifyProposalConsistency(
          adds = range(1, 2) :+ m4,
          removes = range(2, 4),
          observerAdds = range(2, 3),
          observerRemoves = range(1, 2),
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe
        "the same mediators MED::mediator2::m2,MED::mediator4::m4 cannot be added and removed as active in the same proposal, " +
        "the same mediators MED::mediator2::m2 cannot be added as active and observer in the same proposal, " +
        "the same mediators MED::mediator2::m2 cannot be added and removed as observer in the same proposal, " +
        "the same mediators MED::mediator2::m2 cannot be removed as active and observer in the same proposal"
    }
  }

  "MediatorGroupDeltaComputations.verifyProposalAgainstCurrentState" should {

    "succeed with a brand-new MDS with an active and an observer mediator" in {
      MediatorGroupDeltaComputations.verifyProposalAgainstCurrentState(
        None,
        adds = Seq(m1),
        removes = Nil,
        observerAdds = Seq(m2),
        observerRemoves = Nil,
        updateThreshold = None,
      )
    }

    "succeed adding new active and observer mediators to existing MDS" in {
      MediatorGroupDeltaComputations.verifyProposalAgainstCurrentState(
        mds(Seq(m1), Seq(m2)),
        adds = Seq(m3),
        removes = Nil,
        observerAdds = Seq(m4),
        observerRemoves = Nil,
        updateThreshold = None,
      )
    }

    "complain when adding existing active and observer mediators" in {
      MediatorGroupDeltaComputations
        .verifyProposalAgainstCurrentState(
          mds(Seq(m1), Seq(m2, m3)),
          adds = Seq(m1),
          removes = Nil,
          observerAdds = Seq(m2, m3),
          observerRemoves = Nil,
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "mediators MED::mediator1::m1 to be added already active, " +
        "mediators MED::mediator2::m2,MED::mediator3::m3 to be added as observer already observer"
    }

    "complain when removing non-existing active and observer mediators" in {
      MediatorGroupDeltaComputations
        .verifyProposalAgainstCurrentState(
          mds(Seq(m1), Seq(m2, m3)),
          adds = Nil,
          removes = Seq(m2, m3),
          observerAdds = Nil,
          observerRemoves = Seq(m1),
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "mediators MED::mediator2::m2,MED::mediator3::m3 to be removed not active, " +
        "mediators MED::mediator1::m1 to be removed as observer not observer"
    }

    "complain when removing last active mediator" in {
      MediatorGroupDeltaComputations
        .verifyProposalAgainstCurrentState(
          mds(Seq(m1), Seq.empty),
          adds = Nil,
          removes = Seq(m1),
          observerAdds = Nil,
          observerRemoves = Nil,
          updateThreshold = None,
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "mediator group without active mediators"
    }

    "complain when setting threshold too high" in {
      MediatorGroupDeltaComputations
        .verifyProposalAgainstCurrentState(
          mds(Seq(m1), Seq.empty),
          adds = Nil,
          removes = Nil,
          observerAdds = Nil,
          observerRemoves = Nil,
          updateThreshold = Some(PositiveInt.two),
        )
        .leftOrFail(
          "bad proposal"
        ) shouldBe "mediator group threshold 2 larger than active mediator size 1"
    }
  }
}
