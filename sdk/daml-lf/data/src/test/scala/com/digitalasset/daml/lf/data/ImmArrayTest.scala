// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.scalatest.{FlatSpecCheckLaws, Unnatural}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import scalaz.scalacheck.ScalazProperties
import scalaz.std.anyVal._

import ImmArray.ImmArraySeq

class ImmArrayTest extends AnyFlatSpec with Matchers with FlatSpecCheckLaws {
  import DataArbitrary._
  import ImmArraySeq._

  behavior of "toString"

  it should "show nulls" in {
    val sample = ImmArray("hi", null)
    sample.toString shouldBe "ImmArray(hi,null)"
  }

  it should "reverse" in {
    ImmArray(1, 2, 3).reverse shouldBe ImmArray(3, 2, 1)
  }

  it should "copy to array" in {
    val arr: Array[Int] = new Array(5)
    val copied = ImmArray(1, 2, 3, 4, 5).copyToArray(arr)
    copied shouldBe 5
    arr.toSeq shouldBe Seq(1, 2, 3, 4, 5)
  }

  it should "copy to a shorter array" in {
    val arr: Array[Int] = new Array(4)
    val copied = ImmArray(1, 2, 3, 4, 5).copyToArray(arr)
    copied shouldBe 4
    arr.toSeq shouldBe Seq(1, 2, 3, 4)
  }

  it should "copy to a longer array" in {
    val arr: Array[Int] = new Array(6)
    val copied = ImmArray(1, 2, 3, 4, 5).copyToArray(arr)
    copied shouldBe 5
    arr.toSeq shouldBe Seq(1, 2, 3, 4, 5, 0)
  }

  it should "append" in {
    ImmArray(1, 2, 3).slowAppend(ImmArray(4, 5, 6)) shouldBe ImmArray(1, 2, 3, 4, 5, 6)
  }

  it should "uncons" in {
    val arr1 = ImmArray(1, 2)
    val Some((one, arr2)) = ImmArrayCons.unapply[Int](arr1)
    val Some((two, arr3)) = ImmArrayCons.unapply[Int](arr2)
    one shouldBe 1
    two shouldBe 2
    arr2 shouldBe ImmArray(2)
    arr3 shouldBe ImmArray.Empty
    ImmArrayCons.unapply(arr3) shouldBe None
  }

  it should "cons and snoc" in {
    val arr1 = ImmArray(1)
    val arr2 = arr1.slowCons(0)
    arr2 shouldBe ImmArray(0, 1)
    arr2.slowSnoc(2) shouldBe ImmArray(0, 1, 2)
  }

  it should "sort" in {
    ImmArray(2, 1, 3).toSeq.sortBy(identity).toImmArray shouldBe ImmArray(1, 2, 3)
  }

  it should "toString after slice" in {
    ImmArray(1, 2, 3).strictSlice(1, 2).toString shouldBe "ImmArray(2)"
  }

  behavior of "traverse"

  checkLaws(ScalazProperties.traverse.laws[ImmArray])

  it should "work with List as applicative" in {
    import scalaz.syntax.traverse.ToTraverseOps
    import scalaz.std.list.listInstance

    ImmArray(1, 2).traverse(n => (0 to n).toList) shouldBe
      List(
        ImmArray(0, 0),
        ImmArray(0, 1),
        ImmArray(0, 2),
        ImmArray(1, 0),
        ImmArray(1, 1),
        ImmArray(1, 2),
      )
  }

  it should "work with Either as applicative" in {
    import scalaz.syntax.traverse.ToTraverseOps
    import scalaz.std.either.eitherMonad

    type F[A] = Either[Int, A]

    ImmArray(1, 2, 3).traverse[F, Int](n => Right(n)) shouldBe Right(ImmArray(1, 2, 3))
    ImmArray(1, 2, 3).traverse[F, Int](n =>
      if (n >= 2) { Left(n) }
      else { Right(n) }
    ) shouldBe Left(2)
  }

  it should "work with Writer as applicative" in {
    import scalaz.syntax.traverse.ToTraverseOps
    import scalaz.Writer
    import scalaz.WriterT
    import scalaz.std.vector.vectorMonoid

    type F[A] = Writer[Vector[String], A]

    ImmArray(1, 2, 3)
      .traverse[F, Int](n => WriterT.tell(Vector(n.toString)).map { case () => -n })
      .run shouldBe
      ((Vector("1", "2", "3"), ImmArray(-1, -2, -3)))
  }

  it should "work with Function0 as applicative" in {
    import scalaz.syntax.traverse.ToTraverseOps
    import scalaz.std.function.function0Instance

    ImmArray(1, 2, 3)
      .traverse[Function0, String](n => () => n.toString)
      .apply() shouldBe ImmArray("1", "2", "3")
  }

  behavior of "slice"

  it should "slice strictly" in {
    ImmArray[Int](1).strictSlice(0, 0) shouldBe ImmArray.Empty
    ImmArray[Int](1, 2, 3, 4).strictSlice(1, 3) shouldBe ImmArray[Int](2, 3)

    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(0, 2)
    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(1, 1)
    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(-1, 1)
    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(0, -1)
  }

  it should "slice relaxedly" in {
    ImmArray[Int](1).relaxedSlice(1, 2) shouldBe ImmArray.Empty
    ImmArray[Int](1).relaxedSlice(1, 1) shouldBe ImmArray.Empty
    ImmArray[Int](1, 2, 3, 4).relaxedSlice(1, 3) shouldBe ImmArray[Int](2, 3)

    ImmArray[Int](1).relaxedSlice(0, 2) shouldBe ImmArray[Int](1)
    ImmArray[Int](1).relaxedSlice(1, 1) shouldBe ImmArray.Empty
    ImmArray[Int](1).relaxedSlice(-1, 1) shouldBe ImmArray[Int](1)
    ImmArray[Int](1).relaxedSlice(0, -1) shouldBe ImmArray.Empty
  }

  it should "implement equals and hashCode correctly" in {
    val long = ImmArray(1, 2, 3, 4)
    val shortened = long.relaxedSlice(1, 3)

    val short = ImmArray(2, 3)

    shortened.hashCode() shouldBe short.hashCode()
    shortened shouldEqual short
  }

  behavior of "ImmArraySeq"

  it should "use CanBuildFrom of ImmArraySeq" in {
    val seq: ImmArraySeq[String] = ImmArraySeq("hello")
    val stillSeq: ImmArraySeq[String] = seq.map(_ => "hello")
    seq shouldBe stillSeq
    val stillSeqAgain: ImmArraySeq[String] = seq.flatMap(_ => Seq("hello"))
    seq shouldBe stillSeqAgain
  }

  it should "drop correctly" in {
    ImmArraySeq[Int](1).drop(1) shouldBe ImmArraySeq[Int]()
    ImmArraySeq[Int]().drop(1) shouldBe ImmArraySeq[Int]()
  }

  behavior of "Equal instance"

  checkLaws(ScalazProperties.equal.laws[ImmArray[Unnatural[Int]]])

  behavior of "Traverse instance"

  checkLaws(ScalazProperties.traverse.laws[ImmArraySeq])
}
