// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class BoundedOffsetValidationTest
    extends WordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScalaFutures {

  BoundedOffsetValidation.getClass.getSimpleName should {
    "allow empty streams when no bounds" in {
      val sinkF = flowWithEmptySource(None, None)
      whenReady(sinkF)(_ shouldBe List())
    }

    "allow empty streams with upper bound" in {
      val sinkF = flowWithEmptySource(None, Some(17))
      whenReady(sinkF)(_ shouldBe List())
    }

    "allow empty streams with lower bound" in {
      val sinkF = flowWithEmptySource(Some(17), None)
      whenReady(sinkF)(_ shouldBe List())
    }

    "allow empty streams whith both bounds" in {
      val sinkF = flowWithEmptySource(Some(7), Some(17))
      whenReady(sinkF)(_ shouldBe List())
    }

    def flowWithEmptySource(exclusiveLowerBound: Option[Int], inclusiveUpperBound: Option[Int]) = {
      Source
        .empty[Int]
        .via(BoundedOffsetValidation(o => o, exclusiveLowerBound, inclusiveUpperBound))
        .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
        .run()
    }

    val zeroToTen = 0.to(10).toVector

    "allow streams when no bounds" in {
      val sinkF = flowWithSequenceSource(zeroToTen, None, None)
      whenReady(sinkF)(_ shouldBe zeroToTen)
    }

    "allow streams with lower bound not violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, Some(-1), None)
      whenReady(sinkF)(_ shouldBe zeroToTen)
    }

    "disallow streams with lower bound violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, Some(0), None)
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }

    "allow streams with upper bound not violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, None, Some(10))
      whenReady(sinkF)(_ shouldBe zeroToTen)
    }

    "disallow streams with upper bound violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, None, Some(9))
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }

    "allow streams with both bounds not violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, Some(-1), Some(10))
      whenReady(sinkF)(_ shouldBe zeroToTen)
    }

    "disallow streams with both bounds and lower violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, Some(0), Some(10))
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }

    "disallow streams with both bounds and upper violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, Some(-1), Some(9))
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }

    "disallow streams with both bounds violated" in {
      val sinkF = flowWithSequenceSource(zeroToTen, Some(0), Some(9))
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }

    def flowWithSequenceSource(
        elements: Iterable[Int],
        exclusiveLowerBound: Option[Int],
        inclusiveUpperBound: Option[Int]) = {
      Source
        .fromIterator(() => elements.toIterator)
        .via(BoundedOffsetValidation(o => o, exclusiveLowerBound, inclusiveUpperBound))
        .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
        .run()
    }
  }
}
