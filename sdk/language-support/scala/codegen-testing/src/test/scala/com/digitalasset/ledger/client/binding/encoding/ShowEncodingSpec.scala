// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import com.daml.ledger.client.binding.encoding.{LfTypeEncodingSpec => t}
import com.daml.ledger.client.binding.{Primitive => P}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.Show
import scalaz.syntax.show._

class ShowEncodingSpec extends AnyWordSpec with Matchers {

  val alice = P.Party("Alice")

  trait ShowBehavior {
    type A
    implicit val lfenc: LfEncodable[A]
    implicit lazy val show: Show[A] = LfEncodable.encoding[A](ShowEncoding)
  }

  new ShowBehavior {
    type A = t.CallablePayout
    implicit val lfenc: LfEncodable[A] = t.CallablePayout.`the template LfEncodable`

    val contract1 =
      t.CallablePayout(
        alice,
        t.TrialSubRec(10, 100),
        List(1, 2, 3),
        t.TrialEmptyRec(),
        t.TrialVariant.TLeft[P.Text, P.ContractId[t.CallablePayout]]("schön"),
      )

    val contract2 = t.CallablePayout(
      alice,
      t.TrialSubRec(11, 111),
      List(10, 20, 30),
      t.TrialEmptyRec(),
      t.TrialVariant.TRight[P.Text, P.ContractId[t.CallablePayout]](
        P.ContractId("abc123"),
        P.ContractId("def456"),
      ),
    )

    "show t.CallablePayout 1" in {
      val escapedName = "\"sch\\u00F6n\""
      val expected: String =
        s"""MyMain.CallablePayout(receiver = P@"Alice", subr = Trial.SubRec(num = 10, a = 100), lst = [1,2,3], emptyRec = Trial.SubRec(), variant = TLeft($escapedName))"""
      contract1.show.toString should ===(expected)
    }

    "show t.CallablePayout 2" in {
      val expected: String =
        """MyMain.CallablePayout(receiver = P@"Alice", subr = Trial.SubRec(num = 11, a = 111), lst = [10,20,30], emptyRec = Trial.SubRec(), variant = TRight(Trial.Variant.TRight(one = CID@abc123, two = CID@def456)))"""
      contract2.show.toString should ===(expected)
    }
  }
}
