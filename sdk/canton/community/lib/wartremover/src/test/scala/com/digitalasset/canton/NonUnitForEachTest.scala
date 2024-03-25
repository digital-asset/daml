// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.collection.mutable

class NonUnitForEachTest extends AnyWordSpec with Matchers with org.mockito.MockitoSugar {

  private def assertErrors(result: WartTestTraverser.Result, expectedErrors: Int): Assertion = {
    result.errors.length shouldBe expectedErrors
    result.errors.foreach {
      _ should (
        include(NonUnitForEach.messageUnappliedForeach) or
          include(NonUnitForEach.messageAppliedForeach)
      )
    }
    succeed
  }

  "NonUnitForEach" should {

    "detect non-unit values discarded by foreach" in {
      val result = WartTestTraverser(NonUnitForEach) {
        Seq.empty[Int].foreach(i => i)
        List.empty[Int].foreach(i => i)
        Iterable(1).foreach(_ => "abc")
        Iterator(1).foreach(_ => "abc")
        ()
      }
      assertErrors(result, 4)
    }

    "detect non-unit values discarded by tapEach" in {
      val result = WartTestTraverser(NonUnitForEach) {
        Seq.empty[Int].tapEach(i => i)
        List.empty[Int].tapEach(i => i)
        Iterable(1).tapEach(_ => "abc")
        Iterator(1).tapEach(_ => "abc")
        ()
      }
      assertErrors(result, 4)
    }

    "allow unit functions inside foreach and tapEach" in {
      val result = WartTestTraverser(NonUnitForEach) {
        Seq.empty[Int].foreach(_ => ())
        Seq.empty[Int].tapEach(_ => ())
        ()
      }
      result.errors shouldBe empty
    }

    "allow exceptions inside foreach and tapEach" in {
      val result = WartTestTraverser(NonUnitForEach) {
        Seq.empty[Int].foreach(_ => throw new Exception())
        Seq.empty[Int].tapEach(_ => throw new Exception())
        ()
      }
      result.errors shouldBe empty
    }

    "allow custom unit types" in {
      val result = WartTestTraverser(NonUnitForEach) {
        trait T {
          type MyUnit <: Unit
          def myUnit: MyUnit
        }
        class C extends T {
          override type MyUnit = Unit
          override def myUnit: Unit = ()
        }

        val c: T = new C()
        Seq.empty[Int].foreach(_ => c.myUnit)
      }
      result.errors shouldBe empty
    }

    "allow Scala builders" in {
      val result = WartTestTraverser(NonUnitForEach) {
        val builder = Set.newBuilder[Int]
        Seq.empty[Int].foreach { x => builder += x }
        Seq.empty[mutable.Builder[Int, Set[Int]]].foreach { builder => builder += 1 }

        // support pattern-matching functions
        Seq.empty[(mutable.Builder[Int, Set[Int]], Int)].foreach { case (builder, i) =>
          builder += i
        }
        Seq.empty[Either[Int, String]].foreach {
          case Left(_) => throw new Exception
          case Right(_) => builder += 0
        }

        // support if-then
        Seq.empty[Int].foreach { i => if (true) builder += i }
        // support if-then-else
        Seq.empty[Int].foreach { i => if (false) throw new Exception else builder += i }
        Seq.empty[Int].foreach { i =>
          if (false) throw new Exception else if (true) builder += i else ()
        }

        class C {
          val b = Set.newBuilder[Int]
          Seq.empty[Int].foreach { x => b += x }
        }
      }
      result.errors shouldBe empty
    }

    "limitations" should {
      "var builders are flagged" in {
        // This shows the difference with -Wnonunit-statement:
        // Our isThisType does not work for vars.
        val result = WartTestTraverser(NonUnitForEach) {
          var cs = Set.newBuilder[Int]
          cs = Set.newBuilder[Int]
          Seq.empty[Int].foreach { i => cs += i }
        }
        assertErrors(result, 1)
      }
    }

    "detect discarded builders" in {
      val result = WartTestTraverser(NonUnitForEach) {
        def builder = Set.newBuilder[Int]
        Seq.empty[Int].foreach { x => builder += x }
        ()
      }
      assertErrors(result, 1)
    }
  }
}
