// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.{Member, ParticipantId, PartyId, UniqueIdentifier}
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTreeTest extends AnyWordSpec with BaseTest {
  def rec(member: Member): Recipient = MemberRecipient(member)

  lazy val p1: Member = ParticipantId("participant1")
  lazy val p2: Member = ParticipantId("participant2")
  lazy val p3: Member = ParticipantId("participant3")
  lazy val p4: Member = ParticipantId("participant4")
  lazy val p5: Member = ParticipantId("participant5")
  lazy val p6: Member = ParticipantId("participant6")

  lazy val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"alice::party"))
  lazy val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"alice::bob"))
  lazy val pop1: ParticipantsOfParty = ParticipantsOfParty(alice)
  lazy val pop2: ParticipantsOfParty = ParticipantsOfParty(bob)

  lazy val t1 = RecipientsTree.leaf(NonEmpty(Set, p1, p5))
  lazy val t2 = RecipientsTree.leaf(NonEmpty(Set, p3))

  lazy val t3 = RecipientsTree(NonEmpty(Set, rec(p4), rec(p2), pop2), Seq(t1, t2))

  lazy val t4 = RecipientsTree.recipientsLeaf(NonEmpty(Set, rec(p2), rec(p6), pop2))

  lazy val t5 = RecipientsTree(NonEmpty(Set, rec(p1), pop1), Seq(t3, t4))

  "RecipientsTree" when {
    "allRecipients" should {
      "give all recipients" in {
        t5.allRecipients shouldBe Set(p1, p2, p3, p4, p5, p6).map(rec) ++ Set(pop1, pop2)
      }
    }

    "forMember" should {
      "give all subtrees containing the member" in {
        t5.forMember(p2, Set.empty).toSet shouldBe Set(t4, t3)
        t5.forMember(p5, Set(pop2)).toSet shouldBe Set(t4, t3)
      }

      // If a member appears in both the root of a subtree and in the root of a sub-subtree, it receives only the top-level subtree.
      "give only the top-level subtree when there is a subtree and a sub-subtree" in {
        t5.forMember(p1, Set.empty) shouldBe List(t5)
        t5.forMember(p5, Set(pop1)) shouldBe List(t5)
      }
    }
  }

  "serialization and deserialization" should {
    "preserve the same thing" in {

      val serialized = t5.toProtoV0
      val deserialized = RecipientsTree.fromProtoV0(serialized, supportGroupAddressing = true)

      deserialized shouldBe Right(t5)
    }
  }
}
