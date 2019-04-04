// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import scala.concurrent.{ExecutionContext, Future}
import org.scalacheck._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scalaz.{-\/, Equal, Semigroup, Show, \/, \/-}
import slick.lifted.Tag
import SlickTypeEncoding.SupportedProfile

import scala.language.higherKinds

/** A tool for testing the database sanity of multiple codegen-derived
  * classes at the same time.
  *
  * Example, supposing you want to test template types `Foo`, `Bar`, `Baz`:
  *
  * {{{
  *   class MySpec extends MultiTableTests {
  *     import DerivedTemplateTableTest.{fromType => fty}
  *
  *     val profile = ...
  *     val db = ...
  *     // must use val, not def or lazy val
  *     val schemata = derivedTemplateTableTests(profile)(db)(fty[Foo], fty[Bar], fty[Baz])
  *
  *     override protected def beforeAll(): Unit = beforeAllFromSchemata(profile)(db)(schemata)
  *     override protected def afterAll(): Unit = afterAllFromSchemata(profile)(db)(schemata)
  *   }
  * }}}
  */
trait MultiTableTests
    extends AsyncWordSpec
    with ScalaFutures
    with BeforeAndAfterAll
    with MultiTableTests.AsyncShrinkingPropertyChecks {
  import MultiTableTests._
  import DerivedTemplateTableTest.ConstF

  /** Customize random contract building. */
  def genDerivation: LfTypeEncoding.Lt[Gen] = GenEncoding

  /** Customize random contract shrinking on failure. */
  def shrinkDerivation: LfTypeEncoding.Lt[Shrink] = ShrinkEncoding

  final def beforeAllFromSchemata(profile: SupportedProfile)(db: profile.api.Database)(
      schemata: Option[profile.SchemaDescription]) = {
    import profile.api._
    schemata foreach { sd =>
      whenReady(db.run((sd: profile.SchemaActionExtensionMethods).create)) { _ =>
        ()
      }
    }
  }

  final def afterAllFromSchemata(profile: SupportedProfile)(db: profile.api.Database)(
      schemata: Option[profile.SchemaDescription]) = {
    import profile.api._
    schemata foreach { sd =>
      whenReady(db.run((sd: profile.SchemaActionExtensionMethods).drop)) { _ =>
        ()
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  final def derivedTemplateTableTests(
      profile: SupportedProfile)(db: profile.api.Database, sampleSize: Int)(
      tests: DerivedTemplateTableTest[ConstF[Any, ?]]*): Option[profile.SchemaDescription] = {
    require(sampleSize > 0)
    import profile.api._
    type NameTableQuery[A] = (String, TableQuery[profile.Table[A]])
    val tableQueries: Seq[DerivedTemplateTableTest[NameTableQuery]] =
      deriveTestCases(profile, tests) map { dt =>
        val (tableName, tableFun) = dt.data
        dt.withData[NameTableQuery]((tableName, TableQuery(tableFun)))
      }
    val schemata = deriveSchemata(profile)(tableQueries)
    tableQueries foreach { dt =>
      import dt.arb, dt.shr, dt.equal, dt.show
      val (tn, tq) = dt.data
      templateTableTest[dt.A](profile)(db, sampleSize, identity[List[dt.A]])(tn, tq)
    }
    schemata
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def deriveSchemata(profile: SupportedProfile)(
      tests: Seq[DerivedTemplateTableTest[
        ConstF[(_, profile.api.TableQuery[_ <: profile.api.Table[_]]), ?]]])
    : Option[profile.SchemaDescription] = {
    import scalaz.std.iterable._, scalaz.syntax.foldable._, profile.api._
    tests.foldMap1Opt(dt => dt.data._2.schema)(Semigroup instance (_ ++ _))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def deriveTestCases(
      profile: SupportedProfile,
      tests: Seq[DerivedTemplateTableTest[Lambda[a => Any]]])
    : Seq[DerivedTemplateTableTest[Lambda[a => (String, Tag => profile.Table[a])]]] = {
    val encoder = SlickTypeEncoding(profile)
    type NameTable[A] = (String, Tag => profile.Table[A])
    tests map { dt =>
      import dt.lfenc
      dt.withData[NameTable](deriveTestCase(encoder))
    }
  }

  private def deriveTestCase[Profile <: SupportedProfile, A: LfEncodable](
      encoder: SlickTypeEncoding[Profile]): (String, Tag => encoder.profile.Table[A]) =
    encoder.table(LfEncodable.encoding[A](encoder))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  protected def templateTableTest[A: Arbitrary: Shrink: Equal: Show](profile: SupportedProfile)(
      db: profile.api.Database,
      sampleSize: Int,
      resample: List[A] => List[A])(
      tableName: String,
      query: profile.api.TableQuery[_ <: profile.Table[A]]) = {
    import profile.api.{isomorphicType => _, _}

    import com.digitalasset.scalatest.CustomMatcher._
    import scalaz.std.iterable.iterableShow
    import scalaz.std.iterable.iterableEqual
    import scalaz.std.anyVal._
    import scalaz.std.option._

    s"table $tableName" should {
      "query empty table" in {
        for {
          xs <- db.run((query: profile.StreamingQueryActionExtensionMethods[Seq[A], A]).result): Future[
            Seq[A]]
        } yield xs should_=== Seq()
      }

      import Arbitrary.arbitrary
      s"insert $sampleSize contracts, query them back and delete" in forOne(
        Gen.listOfN(sampleSize, arbitrary[A])) { contractSample: List[A] =>
        val contracts = resample(contractSample)
        val insertContracts = (query: profile.InsertActionExtensionMethods[A]) ++= contracts
        val actualSampleSize = contracts.size

        val insertAndSelect = zipSeqFuture(
          db.run(insertContracts),
          db.run((query: profile.StreamingQueryActionExtensionMethods[Seq[A], A]).result): Future[
            Seq[A]])

        def clearAndSelect() =
          zipSeqFuture(
            db.run((query: profile.DeleteActionExtensionMethods).delete),
            db.run((query: profile.StreamingQueryActionExtensionMethods[Seq[A], A]).result): Future[
              Seq[A]]
          )

        for {
          res <- zipSeqFuture(insertAndSelect.recoverWith {
            case ex => clearAndSelect() flatMap (_ => Future.failed(ex))
          }, clearAndSelect())
          ((c1, r1), (c2, r2)) = res
        } yield {
          c1 should_=== Some(actualSampleSize)
          r1.toSet should_=== contracts.toSet
          c2 should_=== actualSampleSize
          r2 should_=== Seq()
        }
      }
    }
  }

  sealed abstract class DerivedTemplateTableTest[+F[_]] {
    type A
    implicit val lfenc: LfEncodable[A]
    implicit lazy val arb: Arbitrary[A] = Arbitrary(LfEncodable.encoding[A](genDerivation))
    implicit lazy val shr: Shrink[A] = LfEncodable.encoding[A](shrinkDerivation)
    implicit lazy val equal: Equal[A] = Equal.equal(LfEncodable.encoding[A](EqualityEncoding))
    implicit lazy val show: Show[A] = LfEncodable.encoding[A](ShowEncoding)
    val data: F[A]

    final def withEncoding(lte: LfTypeEncoding): DerivedTemplateTableTest.Aux[lte.Out, A] =
      withData[lte.Out](lfenc.encoding(lte))

    def withData[G[_]](newData: G[A]): DerivedTemplateTableTest.Aux[G, A]
  }

  object DerivedTemplateTableTest {
    type Aux[+F[_], A0] = DerivedTemplateTableTest[F] {
      type A = A0
    }

    type ConstF[A, P] = A

    def apply[F[_], A0](data0: F[A0])(implicit lfenc0: LfEncodable[A0]): Aux[F, A0] =
      new DerivedTemplateTableTest[F] {
        type A = A0
        override lazy val lfenc = lfenc0
        override val data = data0

        override def withData[G[_]](newData: G[A]) =
          apply(newData)
      }

    def fromType[A: LfEncodable]: Aux[ConstF[Any, ?], A] =
      apply[Lambda[a => Any], A](())
  }
}

object MultiTableTests {

  /** Like [[Future#zip]] but don't evaluate `fb` until `fa` is complete. */
  private def zipSeqFuture[A, B](fa: Future[A], fb: => Future[B])(
      implicit ec: ExecutionContext): Future[(A, B)] =
    for {
      a <- fa
      b <- fb
    } yield (a, b)

  /** Divide-and-conquer `init` into the smallest-length list that fails `f`.
    * `f` is only called when the prior call's future has completed.
    *
    * @return Failing argument and exception, or value if first call succeeded.
    *         Future should never be a failure itself.
    */
  private def minimalFailingSet[T, C[_], Z](init: C[T])(f: C[T] => Future[Z])(
      implicit v: C[T] => Traversable[T],
      b: org.scalacheck.util.Buildable[T, C[T]],
      ec: ExecutionContext): Future[(C[T], Throwable) \/ Z] = {
    import Stream.Empty
    type Failure = (C[T], Throwable)

    def smallestTree(size: Int, remainder: Stream[T]): Future[(Int, Failure)] = {
      assert(size > 0)
      val cremainder = b fromIterable remainder
      val froot = f(cremainder).failed map (root => (size, (cremainder, root)))
      smallest(size, remainder, froot)
    }

    // XXX SC probably should have found an appropriate new tree data structure
    // to replace Stream instead of this fold fusion, oh well
    def smallest(
        size: Int,
        remainder: Stream[T],
        froot: Future[(Int, Failure)]): Future[(Int, Failure)] = {
      remainder match {
        case Empty | _ #:: Empty => froot
        case _ =>
          // we go out of our way to avoid retesting the empty-list case,
          // and to short-circuit when we find a single-element failure
          froot flatMap { root =>
            val split = size / 2
            def fright = smallestTree(size - split, remainder drop split)
            smallestTree(split, remainder take split)
              .transformWith(_.fold(_ => fright, {
                case left @ (ll, _) if ll > 1 =>
                  fright map { case right @ (rl, _) => if (ll <= rl) left else right }
                case left => Future successful left
              }))
              .recover { case _ => root }
          }
      }
    }

    f(init) transformWith (
      _.fold(
        ex => {
          val isize = init.size
          smallest(isize, init.toStream, Future successful ((isize, (init, ex))))
            .map { case (_, failure) => \/.left(failure) }
        },
        z => Future successful \/.right(z)
      )
    )
  }

  /** Shrink elements of `init` in lockstep until `f` succeeds.
    * `f` is only called when the prior call's future has completed.
    * `f` is not called with `init`, which is presumed to be a failure.
    *
    * @return (non-zero) # steps taken, failing argument, and exception,
    *         or failure if the first shrink didn't result in a failure.
    */
  private def minimalFailingElements[T, C[_], Z](init: C[T], shrinkLimit: Int)(
      f: C[T] => Future[Z])(
      implicit v: C[T] => Traversable[T],
      s: Shrink[T],
      b: org.scalacheck.util.Buildable[T, C[T]],
      ec: ExecutionContext): Future[(Int, C[T], Throwable)] = {
    import Stream.Empty
    def rec(count: Int, elements: Traversable[Stream[T]]): Future[(Int, C[T], Throwable)] =
      if (elements forall { case _ #:: Empty => true; case _ => false })
        Future failed (new RuntimeException("test didn't fail on shrunk values"))
      else {
        val newElements = elements map {
          case _ #:: (here @ (_ #:: _)) => here
          case here => here
        }
        val arg = b fromIterable (newElements collect { case h #:: _ => h })
        val trial = f(arg).failed
        if (count >= shrinkLimit) trial map ((count, arg, _))
        else
          trial flatMap { ex =>
            rec(count + 1, newElements) recover { case _ => (count, arg, ex) }
          }
      }
    rec(1, init map (t => t #:: s.shrink(t)))
  }

  /** Shrink `init` by count, and then use `s` to shrink the remaining elements.
    *
    * @return The shrink count, failing argument, and how the future failed,
    *         or Z if success.
    */
  private def shrunkElements[T, C[_], Z](init: C[T], shrinkLimit: Int)(f: C[T] => Future[Z])(
      implicit v: C[T] => Traversable[T],
      s: Shrink[T],
      b: org.scalacheck.util.Buildable[T, C[T]],
      ec: ExecutionContext): Future[(Int, C[T], Throwable) \/ Z] =
    minimalFailingSet(init)(f) flatMap {
      case -\/((ct, ex)) =>
        minimalFailingElements(ct, shrinkLimit)(f) recover { case _ => (0, ct, ex) } map \/.left
      case sz @ \/-(_) => Future successful sz
    }

  trait InformedShrink[A] {
    def iterate[Z](init: A, shrinkLimit: Int)(f: A => Future[Z]): Future[(Int, A, Throwable) \/ Z]
  }

  object InformedShrink {
    implicit def ishrinkContainer1[T: Shrink, C[_]](
        implicit v: C[T] => Traversable[T],
        b: org.scalacheck.util.Buildable[T, C[T]],
        ec: ExecutionContext): InformedShrink[C[T]] =
      new InformedShrink[C[T]] {
        override def iterate[Z](init: C[T], shrinkLimit: Int)(
            f: C[T] => Future[Z]): Future[(Int, C[T], Throwable) \/ Z] =
          shrunkElements(init, shrinkLimit)(f)
      }
  }

  trait AsyncShrinkingPropertyChecks extends Assertions with AsyncTestSuite {
    import org.scalactic.Prettifier
    import org.scalatest.exceptions.StackDepthException
    import org.scalactic.source.Position
    import org.scalatest.exceptions.GeneratorDrivenPropertyCheckFailedException

    protected def defaultShrinkLimit: Int = 5

    def forOne[A](genA: Gen[A])(fun: A => Future[Assertion])(
        implicit shrA: InformedShrink[A],
        showA: Show[A],
        prettifier: Prettifier,
        pos: Position): Future[Assertion] = {
      val initA = genA.sample getOrElse fail("No samples can be produced for A")
      shrA.iterate(initA, defaultShrinkLimit)(fun) map {
        _ valueOr {
          case (shrinks, smallestA, e) =>
            import org.scalatest.exceptions.StackDepth
            val undecorated = s"${e.getClass.getSimpleName} was thrown during property evaluation."
            val beforeSde = s"""$undecorated
              |  Message: ${if (e.getMessage == null) "None" else e.getMessage}"""
            val afterSde = s"""  Occurred when passed generated values (
              |    arg0 = ${showA shows smallestA}${if (shrinks > 0) s" // $shrinks shrinks" else ""}
              |  )"""
            indicateFailure(
              sde =>
                s"""$beforeSde${e match {
                     case sd: StackDepth =>
                       sd.failedCodeFileNameAndLineNumberString.fold("")(fn => s" ($fn)")
                     case _ => ""
                   }}
                 |$afterSde""".stripMargin('|'),
              undecorated,
              List(initA),
              List.empty,
              Some(e),
              pos
            )
        }
      }
    }

    private[this] def indicateFailure(
        messageFun: StackDepthException => String,
        undecoratedMessage: => String,
        scalaCheckArgs: List[Any],
        scalaCheckLabels: List[String],
        optionalCause: Option[Throwable],
        pos: Position): Nothing =
      throw new GeneratorDrivenPropertyCheckFailedException(
        messageFun,
        optionalCause,
        pos,
        None,
        undecoratedMessage,
        scalaCheckArgs,
        None,
        scalaCheckLabels
      )
  }
}
