// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.tailrec

class SeqUtilTest extends AnyWordSpec with BaseTest {

  @tailrec
  final def isPrime(i: Int): Boolean = {
    if (i == Integer.MIN_VALUE) false
    else if (i < 0) isPrime(-i)
    else if (i < 2) false
    else (2 to Math.sqrt(i.toDouble).toInt).forall(d => i % d != 0)
  }

  "splitAfter" should {
    "split after the elements" in {
      SeqUtil.splitAfter(1 to 12)(isPrime) shouldBe
        Seq(
          NonEmpty(Seq, 1, 2),
          NonEmpty(Seq, 3),
          NonEmpty(Seq, 4, 5),
          NonEmpty(Seq, 6, 7),
          NonEmpty(Seq, 8, 9, 10, 11),
          NonEmpty(Seq, 12),
        )
    }

    "handle the empty sequence gracefulle" in {
      SeqUtil.splitAfter(Seq.empty[Int])(_ => true) shouldBe Seq.empty
      SeqUtil.splitAfter(Seq.empty[Int])(_ => false) shouldBe Seq.empty
    }

    "work if no elements satify the predicate" in {
      SeqUtil.splitAfter(1 to 10)(_ >= 11) shouldBe Seq(NonEmpty(Seq, 1, 2 to 10: _*))
    }

    "evaluate the predicate only on arguments" in {
      SeqUtil.splitAfter(1 to 10)(x =>
        if (x >= 1 && x <= 10) x == 5
        else throw new IllegalArgumentException(s"Predicate evaluated on $x")
      ) shouldBe Seq(NonEmpty(Seq, 1, 2, 3, 4, 5), NonEmpty(Seq, 6, 7, 8, 9, 10))
    }
  }
}
