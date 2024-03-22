// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.sequencing.protocol.Recipients.cc
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  lazy val recipients: Recipients = Recipients(NonEmpty(Seq, t5, t2, t3, t5, t6))

  "Recipients" should {

    "filter for a member that doesn't occur" in {
      recipients.forMember(p7) shouldBe None
    }

    "filter for a member that appears in one tree" in {
      recipients.forMember(p6) shouldBe Some(Recipients(NonEmpty(Seq, t6)))
    }

    "filter for a member that appears in several trees" in {
      recipients.forMember(p3) shouldBe Some(Recipients(NonEmpty(Seq, t3, t3, t3)))
    }

    "be preserved through serialization / deserialization" in {
      val proto = recipients.toProtoV0
      val fromProto = Recipients.fromProtoV0(proto)
      fromProto shouldBe Right(recipients)
    }

    "store all recipients" in {
      val all = recipients.allRecipients
      all shouldBe Set(recP1, recP2, recP3, recP4, recP5, recP6)
    }

    "test for a single group when present" in {
      val recipients =
        Recipients(NonEmpty(Seq, RecipientsTree.leaf(NonEmpty.mk(Set, p2, p1, p3))))
      recipients.asSingleGroup shouldBe NonEmpty
        .mk(Set, Recipient(p3), Recipient(p2), Recipient(p1))
        .some
    }

    "test for a single group when not present" in {

      // Multiple trees
      val case1 =
        Recipients(
          NonEmpty(
            List,
            RecipientsTree.leaf(NonEmpty.mk(Set, p2, p1, p3)),
            RecipientsTree.leaf(NonEmpty.mk(Set, p2)),
          )
        )
      case1.asSingleGroup shouldBe None

      // Tree with height > 1
      val case2 =
        Recipients(
          NonEmpty(
            List,
            RecipientsTree(
              NonEmpty.mk(Set, recP2, recP1, recP3),
              Seq(RecipientsTree.leaf(NonEmpty.mk(Set, p1))),
            ),
          )
        )
      case2.asSingleGroup shouldBe None
    }

    "correctly compute leaf members" in {
      val recipients = Recipients(
        NonEmpty(
          List,
          RecipientsTree(
            NonEmpty.mk(Set, participant(1), participant(2)),
            Seq(
              RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, participant(3))),
              RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, participant(4))),
              RecipientsTree(
                NonEmpty.mk(Set, participant(5)),
                Seq(
                  RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, participant(6), participant(2)))
                ),
              ),
            ),
          ),
        )
      )
      recipients.leafRecipients shouldBe
        NonEmpty.mk(Set, recP2, recP3, recP4, recP6)
    }
  }
}

object RecipientsTest {

  def participantRecipient(participant: ParticipantId) = Recipient(participant)

  lazy val p1 = ParticipantId("participant1")
  lazy val p2 = ParticipantId("participant2")
  lazy val p3 = ParticipantId("participant3")
  lazy val p4 = ParticipantId("participant4")
  lazy val p5 = ParticipantId("participant5")
  lazy val p6 = ParticipantId("participant6")
  lazy val p7 = ParticipantId("participant7")
  lazy val p8 = ParticipantId("participant8")
  lazy val p9 = ParticipantId("participant9")
  lazy val p10 = ParticipantId("participant10")
  lazy val p11 = ParticipantId("participant11")
  lazy val p12 = ParticipantId("participant12")
  lazy val p13 = ParticipantId("participant13")
  lazy val p14 = ParticipantId("participant14")
  lazy val p15 = ParticipantId("participant15")
  lazy val p16 = ParticipantId("participant16")
  lazy val p17 = ParticipantId("participant17")
  lazy val p18 = ParticipantId("participant18")

  lazy val recP1 = participantRecipient(p1)
  lazy val recP2 = participantRecipient(p2)
  lazy val recP3 = participantRecipient(p3)
  lazy val recP4 = participantRecipient(p4)
  lazy val recP5 = participantRecipient(p5)
  lazy val recP6 = participantRecipient(p6)
  lazy val recP7 = participantRecipient(p7)
  lazy val recP8 = participantRecipient(p8)

  lazy val t1 = RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, recP1))
  lazy val t2 = RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, recP2))

  lazy val t3 = RecipientsTree(NonEmpty.mk(Set, recP3), Seq(t1, t2))
  lazy val t4 = RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, recP4))

  lazy val t5 = RecipientsTree(NonEmpty.mk(Set, recP5), Seq(t3, t4))

  lazy val t6 = RecipientsTree.recipientsLeaf(NonEmpty.mk(Set, recP6))

  def testInstance: Recipients = {
    val dummyMember = ParticipantId("dummyParticipant")
    cc(dummyMember)
  }

  def participant(i: Int): Recipient = participantRecipient(
    ParticipantId(s"participant$i")
  )

}
