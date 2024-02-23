// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package magnolify.scalacheck.shrink

import org.scalacheck.Shrink
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

class ShrinkDerivationTest extends AnyWordSpec with Matchers {
  import ShrinkDerivationTest.*

  "ShrinkDerivation" should {
    "derive a Shrink instance for a case class" in {
      import magnolify.scalacheck.shrink.semiauto.*

      val shrinkFoo = ShrinkDerivation[Foo]
      val shrunkFoos = shrinkFoo.shrink(Foo(4, -2))
      // Should behave exactly like the existing shrinks for tuples
      val expected = Shrink
        .shrinkTuple2[Int, Int](Shrink.shrinkIntegral[Int], Shrink.shrinkIntegral[Int])
        .shrink((4, -2))
        .map(Foo.tupled)

      shrunkFoos shouldBe expected
    }

    "auto-derive a Shrink for a recursive case class" in {
      // This test works only with fully automatic derivation because
      // because it checks that we pick up the custom DerivedShrink instance for Option.
      // With semi-automatic derivation, magnolia generates its own DerivedShrink instance
      // for Option through which the recursion goes through.
      // The latter does not shrink Some to None though.
      import magnolify.scalacheck.shrink.auto.*

      val shrinkRec = implicitly[Shrink[Rec]]
      val shrunkRecs = shrinkRec.shrink(Rec(4, Some(Rec(1, None))))

      @nowarn("msg=dead code following this construct")
      val expected = Shrink
        .shrinkTuple2[Int, Option[(Int, Option[Nothing])]](
          // Let's be very explicit about the Shrink implicits as auto-derivation is in scope
          Shrink.shrinkIntegral,
          Shrink.shrinkOption(
            Shrink.shrinkTuple2[Int, Option[Nothing]](
              Shrink.shrinkIntegral,
              Shrink.shrinkOption[Nothing](Shrink.shrinkAny[Nothing]),
            )
          ),
        )
        .shrink((4, Some((1, None))))
        .map {
          case (a, None) => Rec(a, None)
          case (a, Some((b, None))) => Rec(a, Some(Rec(b, None)))
          case (_, Some((_, Some(nothing)))) => nothing
        }

      shrunkRecs shouldBe expected
    }

    "semiauto-derive a Shrink for a recursive case class" in {
      // This test uses semi-automatic derivation
      // and therefore generates its own DerivedShrink instance for Option following the sealed-trait construction.
      // Accordingly, Option behaves like an Either[Unit, *].
      import magnolify.scalacheck.shrink.semiauto.*

      val shrinkRec = ShrinkDerivation[Rec]
      val shrunkRecs = shrinkRec.shrink(Rec(4, Some(Rec(1, None))))

      @nowarn("msg=dead code following this construct")
      val expected = Shrink
        .shrinkTuple2[Int, Either[Unit, (Int, Option[Nothing])]](
          Shrink.shrinkIntegral,
          Shrink.shrinkEither(
            Shrink.shrinkAny[Unit],
            Shrink.shrinkTuple2[Int, Option[Nothing]](
              Shrink.shrinkIntegral,
              Shrink.shrinkOption[Nothing](Shrink.shrinkAny[Nothing]),
            ),
          ),
        )
        .shrink((4, Right((1, None))))
        .map {
          case (a, Left(_)) => Rec(a, None)
          case (a, Right((b, None))) => Rec(a, Some(Rec(b, None)))
          case (_, Right((_, Some(nothing)))) => nothing
        }

      shrunkRecs shouldBe expected

    }

    "derive a Shrink for nested case classes" in {
      import magnolify.scalacheck.shrink.auto.*

      val nested = Nested(Foo(4, 0), Rec(-2, None))
      val shrunkNested = Shrink.shrink(nested)

      val expected = Shrink
        .shrinkTuple2[(Int, Int), Int](
          Shrink.shrinkTuple2(Shrink.shrinkIntegral, Shrink.shrinkIntegral),
          Shrink.shrinkIntegral,
        )
        .shrink(((4, 0), -2))
        .map { case ((a, b), c) => Nested(Foo(a, b), Rec(c, None)) }

      shrunkNested shouldBe expected
    }

  }
}

object ShrinkDerivationTest {
  private final case class Foo(a: Int, b: Int)
  private final case class Rec(a: Int, rec: Option[Rec])
  private final case class Nested(a: Foo, b: Rec)
}
