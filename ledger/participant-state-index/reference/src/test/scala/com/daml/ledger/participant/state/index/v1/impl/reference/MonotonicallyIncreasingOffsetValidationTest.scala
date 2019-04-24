// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class MonotonicallyIncreasingOffsetValidationTest
    extends WordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScalaFutures {

  MonotonicallyIncreasingOffsetValidation.getClass.getSimpleName should {
    "allow empty streams" in {
      val sinkF =
        Source
          .empty[Int]
          .via(MonotonicallyIncreasingOffsetValidation(o => o))
          .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
          .run()

      whenReady(sinkF)(_ shouldBe List())
    }

    "allow monotonic increasing streams" in {
      val elemsThatPassThrough = 0.to(10).toVector
      val sinkF = processElements(elemsThatPassThrough)
      whenReady(sinkF)(_ shouldBe elemsThatPassThrough)
    }

    "disallow monotonic non-decreasing streams" in {
      val elemsThatFlattenDontPassThrough = 0.to(10).toVector :+ 10
      val sinkF = processElements(elemsThatFlattenDontPassThrough)
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }

    "disallow decreasing streams" in {
      val elemsThatDipDontPassThrough = 0.to(10).reverse.toVector
      val sinkF = processElements(elemsThatDipDontPassThrough)
      whenReady(sinkF.failed)(_ shouldBe a[RuntimeException])
    }
  }

  private def processElements(elements: Iterable[Int]) = {
    Source
      .fromIterator(() => elements.iterator)
      .via(MonotonicallyIncreasingOffsetValidation(o => o))
      .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
      .run()
  }
}
