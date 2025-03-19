// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.LazyValWithContextTest.{
  ClassUsingLazyValWithContext,
  ClassUsingRecursiveLazyValWithContext,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}

class LazyValWithContextTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "LazyValWithContext" should {
    "return the result of the initializer" in {
      val sut = ClassUsingLazyValWithContext(_ => "abc")
      sut.lazyVal(1) shouldBe "abc"
    }

    "evaluate the initializer only once" in {
      val counter = new AtomicInteger()
      val sut = ClassUsingLazyValWithContext { _ =>
        counter.incrementAndGet().discard[Int]
        "abc"
      }
      sut.lazyVal(1) shouldBe "abc"
      sut.lazyVal(2) shouldBe "abc"
      counter.get() shouldBe 1
    }

    "initialize the value with the first context value" in {
      val sut = ClassUsingLazyValWithContext { i =>
        i.toString
      }
      sut.lazyVal(1) shouldBe "1"
      sut.lazyVal(2) shouldBe "1"
    }

    "retry initialization upon an exception" in {
      val sut = ClassUsingLazyValWithContext { i =>
        if (i == 1) throw new IllegalArgumentException() else "abc"
      }
      an[IllegalArgumentException] should be thrownBy sut.lazyVal(1)
      sut.lazyVal(2) shouldBe "abc"
    }

    "initialize only once even under contention" in {
      val sutCell = new SingleUseCell[ClassUsingLazyValWithContext]()
      val stash = Promise[String]()
      val sut = ClassUsingLazyValWithContext { i =>
        if (i == 1) {
          // Spawn another thread that tries to access the lazy val while it's being initialized
          // and wait a bit so that it actually runs
          stash.completeWith(Future {
            sutCell.get.value.lazyVal(2)
          })
          Threading.sleep(100)
          "abc"
        } else {
          "def"
        }
      }
      sutCell.putIfAbsent(sut).discard
      sut.lazyVal(1) shouldBe "abc"
      stash.future.futureValue shouldBe "abc"
    }

    "initialize multiple times when the initializer recurses" in {
      // This test shows that the `synchronized` block is re-entrant.
      // The same behavior can be observed with ordinary lazy vals in Scala.
      val sut = new ClassUsingRecursiveLazyValWithContext()
      sut.lazyVal(2) shouldBe "foo2"
      sut.state.get shouldBe 2
    }
  }
}

object LazyValWithContextTest {
  private type Context = Int
  private type T = String

  private class ClassUsingLazyValWithContext(initializer: Context => T) {
    val _lazyVal: LazyValWithContext[T, Context] = new LazyValWithContext[T, Context](initializer)
    def lazyVal(context: Int): String = _lazyVal.get(context)
  }
  private object ClassUsingLazyValWithContext {
    def apply(initializer: Context => T): ClassUsingLazyValWithContext =
      new ClassUsingLazyValWithContext(initializer)
  }

  private class ClassUsingRecursiveLazyValWithContext {
    val state = new AtomicInteger(0)

    val _lazyVal: LazyValWithContext[T, Context] = new LazyValWithContext[T, Context]({ context =>
      val newState = state.incrementAndGet()
      if (newState == 1) lazyVal(context) else s"foo$newState"
    })
    def lazyVal(context: Int): String = _lazyVal.get(context)
  }
}
