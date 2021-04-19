// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.ledger.api.domain.CommandId
import com.daml.lf.data.Ref

import java.util.UUID
import scala.util.Random
import scalaz.syntax.tag._

class DeduplicationKeyMakerSpec extends AnyWordSpec with Matchers {

  val commandId: CommandId = CommandId(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))

  "DeduplicationKeyMaker" should {
    "make a deduplication key starting with a command ID in plain-text" in {
      DeduplicationKeyMaker.make(commandId, List(aParty())) should startWith(commandId.unwrap)
    }

    "make a deduplication key with a limited length for a large number of submitters" in {
      val submitters = (1 to 50).map(_ => aParty()).toList
      val MaxKeyLength = 200
      DeduplicationKeyMaker.make(commandId, submitters).length should be < MaxKeyLength
    }

    "make the same deduplication key for submitters of different order" in {
      val submitter1 = aParty()
      val submitter2 = aParty()
      val submitter3 = aParty()

      val key1 = DeduplicationKeyMaker.make(commandId, List(submitter1, submitter2, submitter3))
      val key2 = DeduplicationKeyMaker.make(commandId, List(submitter1, submitter3, submitter2))

      key1 shouldBe key2
    }

    "make the same deduplication key for duplicated submitters" in {
      val submitter1 = aParty()
      val submitter2 = aParty()

      val key1 = DeduplicationKeyMaker.make(commandId, List(submitter1, submitter2))
      val key2 = DeduplicationKeyMaker.make(
        commandId,
        List(submitter1, submitter1, submitter2, submitter2, submitter2),
      )

      key1 shouldBe key2
    }

    def aParty(): Ref.Party = Ref.Party.assertFromString(Random.alphanumeric.take(100).mkString)
  }
}
