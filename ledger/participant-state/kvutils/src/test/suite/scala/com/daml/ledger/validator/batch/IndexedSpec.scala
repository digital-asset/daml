// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.mockito.MockitoSugar

import scala.concurrent.Future

class IndexedSpec extends AsyncWordSpec with Matchers with Inside with MockitoSugar {

  "indexed" should {

    "map over value" in {
      val indexed = Indexed(50, 0L)
      val indexed2 = indexed.map(_ + 1)
      indexed2.index should be(0L)
      indexed2.value should be(51)
    }

    "mapFuture over value" in {
      val indexed = Indexed(50, 0L)
      indexed
        .mapFuture(v => Future { v + 1 })
        .map { newIndexed =>
          newIndexed.index should be(0L)
          newIndexed.value should be(51)
        }
    }

    "fromSeq works" in {
      val seq = Seq(1, 2, 3)
      val indexedSeq = Indexed.fromSeq(seq)
      indexedSeq should have size (3)
      seq.zipWithIndex.foreach {
        case (x, i) =>
          indexedSeq(i).value should be(x)
          indexedSeq(i).index should be(i)
      }
      succeed
    }
  }
}
