// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util.akkastreams

import akka.stream.scaladsl.{Keep, Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ExtractSingleMaterializedValueTest
    extends WordSpec
    with Matchers
    with ScalaFutures
    with AkkaBeforeAndAfterAll {

  private val discriminator = { i: Int =>
    if (i < 0) Some(i) else None
  }

  private val elemsThatPassThrough = 0.to(10).toVector

  ExtractMaterializedValue.getClass.getSimpleName when {

    "there's a single valid value" should {
      "extract it" in {
        val elemToExtract = -1

        val elements = elemToExtract +: elemsThatPassThrough
        val (extractedF, restF) = processElements(Random.shuffle(elements))

        whenReady(extractedF)(_ shouldEqual elemToExtract)
        whenReady(restF)(_ should contain theSameElementsAs elements)
      }
    }

    "there are multiple valid values" should {
      "extract the first matching element" in {
        val elemToExtract = -1
        val otherCandidateShuffledIn = -2

        val elements = elemToExtract +: Random.shuffle(
          otherCandidateShuffledIn +: elemsThatPassThrough)
        val (extractedF, restF) = processElements(elements)

        whenReady(extractedF)(_ shouldEqual elemToExtract)
        whenReady(restF)(_ should contain theSameElementsAs elements)
      }
    }

    "there are no valid values" should {
      "fail the materialized future, but let the stream continue otherwise" in {

        val (extractedF, restF) =
          processElements(Random.shuffle(elemsThatPassThrough))

        whenReady(extractedF.failed)(_ shouldBe a[RuntimeException])
        whenReady(restF)(_.sorted shouldEqual elemsThatPassThrough)
      }
    }

  }

  private def processElements(elements: Iterable[Int]) = {
    Source
      .fromIterator(() => elements.iterator)
      .viaMat(ExtractMaterializedValue(discriminator))(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()
  }
}
