// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.Checkers
import org.scalacheck.{Arbitrary, Gen, Properties}
import scalaz.Equal
import scalaz.scalacheck.ScalazProperties
import scalaz.std.anyVal._

class ImmArrayTest extends FlatSpec with Matchers with Checkers {
  import ImmArrayTest._

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
    ImmArray(1, 2, 3, 4, 5).copyToArray(arr)
    arr.toSeq shouldBe Array[Int](1, 2, 3, 4, 5).toSeq
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
    arr3 shouldBe ImmArray.empty[Int]
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

    ImmArray(1, 2).traverseU(n => (0 to n).toList) shouldBe
      List(
        ImmArray(0, 0),
        ImmArray(0, 1),
        ImmArray(0, 2),
        ImmArray(1, 0),
        ImmArray(1, 1),
        ImmArray(1, 2))
  }

  it should "work with Either as applicative" in {
    import scalaz.syntax.traverse.ToTraverseOps
    import scalaz.std.either.eitherMonad

    type F[A] = Either[Int, A]

    ImmArray(1, 2, 3).traverse[F, Int](n => Right(n)) shouldBe Right(ImmArray(1, 2, 3))
    ImmArray(1, 2, 3).traverse[F, Int](n => if (n >= 2) { Left(n) } else { Right(n) }) shouldBe Left(
      2)
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
      .apply shouldBe ImmArray("1", "2", "3")
  }

  behavior of "slice"

  it should "slice strictly" in {
    ImmArray[Int](1).strictSlice(0, 0) shouldBe ImmArray.empty[Int]
    ImmArray[Int](1, 2, 3, 4).strictSlice(1, 3) shouldBe ImmArray[Int](2, 3)

    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(0, 2)
    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(1, 1)
    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(-1, 1)
    an[IndexOutOfBoundsException] should be thrownBy ImmArray[Int](1).strictSlice(0, -1)
  }

  it should "slice relaxedly" in {
    ImmArray[Int](1).relaxedSlice(1, 2) shouldBe ImmArray.empty[Int]
    ImmArray[Int](1).relaxedSlice(1, 1) shouldBe ImmArray.empty[Int]
    ImmArray[Int](1, 2, 3, 4).relaxedSlice(1, 3) shouldBe ImmArray[Int](2, 3)

    ImmArray[Int](1).relaxedSlice(0, 2) shouldBe ImmArray[Int](1)
    ImmArray[Int](1).relaxedSlice(1, 1) shouldBe ImmArray.empty[Int]
    ImmArray[Int](1).relaxedSlice(-1, 1) shouldBe ImmArray[Int](1)
    ImmArray[Int](1).relaxedSlice(0, -1) shouldBe ImmArray.empty[Int]
  }

  behavior of "ImmArraySeq"

  it should "use CanBuildFrom of ImmArraySeq" in {
    val seq = ImmArray.ImmArraySeq("hello")
    val stillSeq: ImmArray.ImmArraySeq[String] = seq.map(_ => "hello")
    seq shouldBe stillSeq
  }

  it should "drop correctly" in {
    ImmArray.ImmArraySeq[Int](1).drop(1) shouldBe ImmArray.ImmArraySeq[Int]()
    ImmArray.ImmArraySeq[Int]().drop(1) shouldBe ImmArray.ImmArraySeq[Int]()
  }

  behavior of "Equal instance"

  checkLaws(ScalazProperties.equal.laws[ImmArray[IntInt]])

  private def checkLaws(props: Properties) =
    props.properties foreach { case (s, p) => it should s in check(p) }
}

object ImmArrayTest {
  private final case class IntInt(i: Int)
  private implicit val arbII: Arbitrary[IntInt] = Arbitrary(Arbitrary.arbitrary[Int] map IntInt)
  private implicit val eqII: Equal[IntInt] = Equal.equal(_ == _)

  implicit def arbImmArray[A: Arbitrary]: Arbitrary[ImmArray[A]] =
    Arbitrary {
      for {
        raw <- Arbitrary.arbitrary[Seq[A]]
        min <- Gen.choose(0, 0 max (raw.size - 1))
        max <- Gen.choose(min, raw.size)
      } yield if (min >= max) ImmArray(Seq()) else ImmArray(raw).strictSlice(min, max)
    }
}
