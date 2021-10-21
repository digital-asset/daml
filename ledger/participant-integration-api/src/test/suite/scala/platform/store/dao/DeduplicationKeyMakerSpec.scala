// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.ledger.api.domain.CommandId
import com.daml.lf.data.Ref
import com.daml.platform.store.appendonlydao.DeduplicationKeyMaker
import org.scalatest.prop.TableDrivenPropertyChecks

import java.util.UUID
import scala.util.Random
import scalaz.syntax.tag._

class DeduplicationKeyMakerSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  val commandId: CommandId = CommandId(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))

  "DeduplicationKeyMaker" should {
    "make a deduplication key starting with a command ID in plain-text" in {
      DeduplicationKeyMaker.make(commandId, List(aParty())) should startWith(commandId.unwrap)
    }

    "make different keys for different sets of submitters" in {
      val aCommonParty = aParty()
      val cases = Table(
        ("Submitters for key1", "Submitters for key2"),
        (List(aParty()), List(aParty())),
        (List(aCommonParty, aParty()), List(aCommonParty, aParty())),
        (List(aParty(), aParty()), List(aParty(), aParty())),
      )

      forAll(cases) { case (key1Submitters, key2Submitters) =>
        val key1 = DeduplicationKeyMaker.make(commandId, key1Submitters)
        val key2 = DeduplicationKeyMaker.make(commandId, key2Submitters)

        key1 shouldNot equal(key2)
      }
    }

    "make a deduplication key with a limited length for a large number of submitters" in {
      val submitters = (1 to 50).map(_ => aParty()).toList

      /** The motivation for the MaxKeyLength is to avoid problems with putting deduplication key in a database
        * index (e.g. for Postgres the limit for the index row size is 2712).
        * The value 200 is set arbitrarily to provide some space for other data.
        */
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
