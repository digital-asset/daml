// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.{Applicative, Parallel}
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.Semaphore
import scala.collection.immutable.ArraySeq
import scala.concurrent.{Future, blocking}

class TraverseTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "traverse" when {
    futureLikes.foreach { fOps =>
      s"used with ${fOps.name}" should {
        "deadlock" in {
          deadlockTraverse(fOps)
        }
      }
    }

    // The following tests merely document the implementation choices made by a particular version of Cats
    "used on a specific Seq" should {
      "preserve the Seq type" in {
        List(1).traverse(Option.apply).value shouldBe a[List[_]]
        Vector(1).traverse(Option.apply).value shouldBe a[Vector[_]]
        ArraySeq(1).traverse(Option.apply).value shouldBe an[ArraySeq[_]]
        LazyList(1).traverse(Option.apply).value shouldBe a[LazyList[_]]
      }
    }

    "used on a general Seq" should {
      "convert everything to Vector" in {
        def go(xs: Seq[Int]): Assertion =
          xs.traverse(Option.apply).value shouldBe a[Vector[_]]

        go(List(1))
        go(Vector(1))
        go(ArraySeq(1))
        go(LazyList(1))
      }
    }

    "used on a specific NonEmpty Seq" should {
      "preserve the Seq type" in {
        NonEmpty(List, 1).toNEF.traverse(Option.apply).value.forgetNE shouldBe a[List[_]]
        NonEmpty(Vector, 1).toNEF.traverse(Option.apply).value.forgetNE shouldBe a[Vector[_]]
        NonEmpty(ArraySeq, 1).toNEF.traverse(Option.apply).value.forgetNE shouldBe an[ArraySeq[_]]
        NonEmpty(LazyList, 1).toNEF.traverse(Option.apply).value.forgetNE shouldBe a[LazyList[_]]
      }
    }

    "used on a generic NonEmpty Seq" should {
      "convert everything to Vector" in {
        def go(xs: NonEmpty[Seq[Int]]): Assertion =
          xs.toNEF
            .traverse(Option.apply)
            .value
            .forgetNE shouldBe a[Vector[_]]

        go(NonEmpty(List, 1))
        go(NonEmpty(Vector, 1))
        go(NonEmpty(ArraySeq, 1))
        go(NonEmpty(LazyList, 1))
      }
    }
  }

  "traverse_" when {
    futureLikes.foreach { fOps =>
      s"used with ${fOps.name}" should {
        "deadlock" in {
          deadlockTraverse_(fOps)
        }
      }
    }
  }

  "flatTraverse" when {
    futureLikes.foreach { fOps =>
      s"used with ${fOps.name}" should {
        "deadlock" in {
          deadlockFlatTraverse(fOps)
        }
      }
    }
  }

  "parTraverse" when {
    futureLikes.foreach { fOps =>
      s"used with ${fOps.name}" should {
        "run in parallel" in {
          runParTraverse(fOps)
        }

        "be stack safe" in {
          parTraverseStackSafety(fOps)
        }
      }
    }
  }

  "parTraverse_" when {
    futureLikes.foreach { fOps =>
      s"used with ${fOps.name}" should {
        "run in parallel" in {
          runParTraverse_(fOps)
        }

        "be stack safe" in {
          parTraverse_StackSafety(fOps)
        }
      }
    }
  }

  "parFlatTraverse" when {
    futureLikes.foreach { fOps =>
      s"used with ${fOps.name}" should {
        "run in parallel" in {
          runParFlatTraverse(fOps)
        }

        "be stack safe" in {
          parFlatTraverseStackSafety(fOps)
        }
      }
    }
  }

  private case object FutureOps extends TraverseTest.FutureLikeOps {
    override type F[X] = Future[X]
    override def name: String = "Future"
    override def mk[A](x: => A): Future[A] = Future(x)
    override def isCompleted[A](f: Future[A]): Boolean = f.isCompleted
    override def await[A](f: Future[A]): A = f.futureValue
    override def applicative: Applicative[Future] = Applicative[Future]
    override def parallel: Parallel[Future] = Parallel[Future]
  }

  private case object FutureUnlessShutdownOps extends TraverseTest.FutureLikeOps {
    override type F[X] = FutureUnlessShutdown[X]
    override def name: String = "FutureUnlessShutdown"
    override def mk[A](x: => A): FutureUnlessShutdown[A] = FutureUnlessShutdown.outcomeF(Future(x))
    override def isCompleted[A](f: FutureUnlessShutdown[A]): Boolean =
      f.unwrap.isCompleted
    override def await[A](f: FutureUnlessShutdown[A]): A =
      f.onShutdown(fail("shutdown")).futureValue
    override def applicative: Applicative[FutureUnlessShutdown] = Applicative[FutureUnlessShutdown]
    override def parallel: Parallel[FutureUnlessShutdown] = Parallel[FutureUnlessShutdown]
  }

  private case object EitherTFutureOps extends TraverseTest.FutureLikeOps {
    override type F[X] = EitherT[Future, String, X]
    override def name: String = "EitherT[Future, String, *]"
    override def mk[A](x: => A): EitherT[Future, String, A] = EitherT(Future(Either.right(x)))
    override def isCompleted[A](f: EitherT[Future, String, A]): Boolean = f.value.isCompleted
    override def await[A](f: EitherT[Future, String, A]): A = f.valueOrFail("left").futureValue
    override def applicative: Applicative[EitherT[Future, String, *]] =
      Applicative[EitherT[Future, String, *]]
    override def parallel: Parallel[EitherT[Future, String, *]] =
      Parallel[EitherT[Future, String, *]]
  }

  private lazy val futureLikes = Seq(
    FutureOps,
    FutureUnlessShutdownOps,
    EitherTFutureOps,
  )

  private sealed trait Op extends Product with Serializable
  private case object Acquire extends Op
  private case object Release extends Op

  private def runOp(semaphore: Semaphore, name: String, op: Op): Unit = {
    op match {
      case Acquire =>
        logger.debug(s"Acquiring $semaphore for $name")
        blocking { semaphore.acquire() }
      case Release =>
        logger.debug(s"Releasing $semaphore for $name")
        semaphore.release()
    }
  }

  // Put the two acquires at the end and the releases into the middle so that we detect sequential
  // processing even if it's done from right to left.
  private lazy val ops = List(Acquire, Release, Release, Acquire)

  private def checkDeadlock[F[_]](mkF: Semaphore => F[Unit])(
      isCompleted: F[Unit] => Boolean
  ): F[Unit] = {
    val semaphore = new Semaphore(3)
    semaphore.acquire(3)
    val fl = mkF(semaphore)
    always() { isCompleted(fl) shouldBe false }
    logger.debug("Releasing the deadlock")
    semaphore.release()
    eventually() { isCompleted(fl) shouldBe true }
    semaphore.release(2)
    fl
  }

  private def deadlockTraverse(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.applicative
    val fl = checkDeadlock[fOps.F](semaphore =>
      ops.traverse { op => fOps.mk(runOp(semaphore, s"traverse on ${fOps.name}", op)) }.void
    )(fOps.isCompleted)
    fOps.await(fl)
  }

  private def deadlockTraverse_(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.applicative
    val fl = checkDeadlock[fOps.F](semaphore =>
      ops.traverse_ { op => fOps.mk(runOp(semaphore, s"traverse_ on ${fOps.name}", op)) }
    )(fOps.isCompleted)
    fOps.await(fl)
  }

  private def deadlockFlatTraverse(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.applicative
    val fl = checkDeadlock[fOps.F](semaphore =>
      ops.flatTraverse { op =>
        fOps.mk(List(runOp(semaphore, s"flatTraverse on ${fOps.name}", op)))
      }.void
    )(fOps.isCompleted)
    fOps.await(fl)
  }

  private def runParTraverse(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.parallel
    val semaphore = new Semaphore(2)
    semaphore.acquire(2)
    val fl = ops.parTraverse { op => fOps.mk(runOp(semaphore, "parTraverse", op)) }
    fOps.await(fl) should have size ops.size.toLong
    semaphore.release(2)
  }

  private def runParTraverse_(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.parallel
    val semaphore = new Semaphore(2)
    semaphore.acquire(2)
    val fl = ops.parTraverse_ { op => fOps.mk(runOp(semaphore, "parTraverse_", op)) }
    fOps.await(fl)
    semaphore.release(2)
  }

  private def runParFlatTraverse(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.parallel
    val semaphore = new Semaphore(2)
    semaphore.acquire(2)
    val fl = ops.parFlatTraverse { op => fOps.mk(List(runOp(semaphore, "parFlatTraverse", op))) }
    fOps.await(fl) should have size ops.size.toLong
    semaphore.release(2)
  }

  private lazy val stackSafetyDepth = 20000

  private def parTraverseStackSafety(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.parallel
    val fl = (1 to stackSafetyDepth: Seq[Int]).parTraverse(_i => fOps.mk(()))
    fOps.await(fl) should have size stackSafetyDepth.toLong
  }

  private def parTraverse_StackSafety(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.parallel
    val fl = (1 to stackSafetyDepth: Seq[Int]).parTraverse_(_i => fOps.mk(()))
    fOps.await(fl)
  }

  private def parFlatTraverseStackSafety(fOps: TraverseTest.FutureLikeOps): Unit = {
    import fOps.parallel
    val fl = (1 to stackSafetyDepth: Seq[Int]).parFlatTraverse(_i => fOps.mk(Seq(())))
    fOps.await(fl) should have size stackSafetyDepth.toLong
  }
}

object TraverseTest {
  private[TraverseTest] trait FutureLikeOps extends Product with Serializable {
    type F[_]
    def name: String
    def mk[A](x: => A): F[A]
    def isCompleted[A](f: F[A]): Boolean
    def await[A](f: F[A]): A
    implicit def applicative: Applicative[F]
    implicit def parallel: Parallel[F]
  }
}
