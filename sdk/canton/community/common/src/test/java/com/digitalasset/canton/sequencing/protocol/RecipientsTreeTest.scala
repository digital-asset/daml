// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.topology.{Member, ParticipantId}
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTreeTest extends AnyWordSpec with BaseTest {
  def rec(member: Member): Recipient = MemberRecipient(member)

  private lazy val p1: Member = ParticipantId("participant1")
  private lazy val p2: Member = ParticipantId("participant2")
  private lazy val p3: Member = ParticipantId("participant3")
  private lazy val p4: Member = ParticipantId("participant4")
  private lazy val p5: Member = ParticipantId("participant5")
  private lazy val p6: Member = ParticipantId("participant6")

  private lazy val mod1: MediatorGroupRecipient = MediatorGroupRecipient(NonNegativeInt.zero)
  private lazy val mod2: MediatorGroupRecipient = MediatorGroupRecipient(NonNegativeInt.one)

  private lazy val t1 = RecipientsTree.leaf(NonEmpty(Set, p1, p5))
  private lazy val t2 = RecipientsTree.leaf(NonEmpty(Set, p3))

  private lazy val t3 = RecipientsTree(NonEmpty(Set, rec(p4), rec(p2), mod2), Seq(t1, t2))

  private lazy val t4 = RecipientsTree.recipientsLeaf(NonEmpty(Set, rec(p2), rec(p6), mod2))

  private lazy val t5 = RecipientsTree(NonEmpty(Set, rec(p1), mod1), Seq(t3, t4))

  "RecipientsTree" when {
    "allRecipients" should {
      "give all recipients" in {
        t5.allRecipients shouldBe Set(p1, p2, p3, p4, p5, p6).map(rec) ++ Set(mod1, mod2)
      }
    }

    "forMember" should {
      "give all subtrees containing the member" in {
        t5.forMember(p2, Set.empty).toSet shouldBe Set(t4, t3)
        t5.forMember(p5, Set(mod2)).toSet shouldBe Set(t4, t3)
      }

      // If a member appears in both the root of a subtree and in the root of a sub-subtree, it receives only the top-level subtree.
      "give only the top-level subtree when there is a subtree and a sub-subtree" in {
        t5.forMember(p1, Set.empty) shouldBe List(t5)
        t5.forMember(p5, Set(mod1)) shouldBe List(t5)
      }
    }

    "allPaths" should {
      "give all paths within the tree" in {
        t5.allPaths shouldBe Seq(
          Seq(Set(rec(p1), mod1), Set(rec(p4), rec(p2), mod2), Set(rec(p1), rec(p5))),
          Seq(Set(rec(p1), mod1), Set(rec(p4), rec(p2), mod2), Set(rec(p3))),
          Seq(Set(rec(p1), mod1), Set(rec(p2), rec(p6), mod2)),
        )
      }
    }
  }

  "serialization and deserialization" should {
    "preserve the same thing" in {

      val serialized = t5.toProtoV30
      val deserialized = RecipientsTree.fromProtoV30(serialized)

      deserialized shouldBe Right(t5)
    }
  }
}
