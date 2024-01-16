// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import com.daml.ledger.client.binding.encoding.{LfTypeEncodingSpec => t}
import com.daml.ledger.client.binding.{Primitive => P}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.equal._

class EqualityEncodingSpec extends AnyWordSpec with Matchers {
  val alice = P.Party("Alice")

  trait EqualityEncodingBehavior {
    type A
    implicit val lfenc: LfEncodable[A]
    lazy val equality: EqualityEncoding.Fn[A] = LfEncodable.encoding[A](EqualityEncoding)
    implicit lazy val ` show`: scalaz.Show[A] = LfEncodable.encoding[A](ShowEncoding)
    implicit lazy val ` equal`: scalaz.Equal[A] = equality.apply(_, _)
  }

  new EqualityEncodingBehavior {
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

    "t.CallablePayout equality" in {
      contract1 assert_=== contract1
      contract1 assert_=== contract1.copy()

      equality.apply(contract1, contract1.copy()) should ===(true)
      equality.apply(contract1, contract1.copy(subr = t.TrialSubRec(11, 101))) should ===(false)
      equality.apply(contract1, contract1.copy(lst = List())) should ===(false)
      equality.apply(contract1, contract1.copy(lst = List(3, 2, 1))) should ===(false)
      equality.apply(contract1, contract1.copy(lst = List(1, 2, 3, 4))) should ===(false)

      equality.apply(contract2, contract2) should ===(true)
      equality.apply(contract2, contract2.copy()) should ===(true)
      equality.apply(contract2, contract2.copy(variant = t.TrialVariant.TLeft("schön"))) should ===(
        false
      )
      equality.apply(
        contract2,
        contract2.copy(variant =
          t.TrialVariant.TRight(P.ContractId("ABC123"), P.ContractId("DEF456"))
        ),
      ) should ===(false)

      equality.apply(contract1, contract2) should ===(false)
    }
  }
}
