// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.annotation.tailrec
import scala.collection.{IterableOps, immutable}
import scala.concurrent.{ExecutionContext, Future}

object IterableUtil {

  /** Split an iterable into a lazy Stream of consecutive elements with the same value of `f(element)`.
    */
  def spansBy[A, CC[X] <: immutable.Iterable[X], C, B](
      iterable: IterableOps[A, CC, C & immutable.Iterable[A]]
  )(f: A => B): LazyList[(B, NonEmpty[CC[A]])] = {
    val iterator = iterable.iterator

    def peek(): Option[(A, B)] =
      Option.when(iterator.hasNext) {
        val a = iterator.next()
        val b = f(a)
        a -> b
      }

    def generateBlocks(state: Option[(A, B)]): LazyList[(B, NonEmpty[CC[A]])] = {
      state match {
        case Some((head, b)) =>
          val blockBuilder = iterable.iterableFactory.newBuilder[A]
          blockBuilder.addOne(head)

          @tailrec
          def addAllSame(): Option[(A, B)] = {
            peek() match {
              case Some((next, bNext)) if b == bNext =>
                blockBuilder.addOne(next)
                addAllSame()
              case peeked => peeked
            }
          }

          val stateForNextBlock = addAllSame()
          (b -> NonEmptyUtil.fromUnsafe(blockBuilder.result())) #::
            generateBlocks(stateForNextBlock)
        case None => LazyList.empty
      }
    }

    generateBlocks(peek())
  }

  /** Splits the sequence `xs` after each element that satisfies `p` and returns the sequence of chunks */
  def splitAfter[A, CC[X] <: immutable.Iterable[X], C, B](
      xs: IterableOps[A, CC, C & immutable.Iterable[A]]
  )(p: A => Boolean): LazyList[NonEmpty[CC[A]]] = {
    val iterator = xs.iterator

    def go(): LazyList[NonEmpty[CC[A]]] = {
      if (iterator.hasNext) {
        val chunkBuilder = xs.iterableFactory.newBuilder[A]

        @tailrec def addUntilP(): Unit = {
          if (iterator.hasNext) {
            val x = iterator.next()
            chunkBuilder.addOne(x)
            if (!p(x)) addUntilP()
          }
        }

        addUntilP()
        val block = chunkBuilder.result()
        NonEmptyUtil.fromUnsafe(block) #:: go()
      } else LazyList.empty
    }

    go()
  }

  /** Returns the zipping of `elems` with `seq` where members `y` of `seq` are skipped if `!by(x, y)`
    * for the current member `x` from `elems`. Zipping stops when there are no more elements in `elems` or `seq`
    */
  def subzipBy[A, B, C](elems: Iterator[A], seq: Iterator[B])(by: (A, B) => Option[C]): Seq[C] = {
    val zipped = Seq.newBuilder[C]

    @tailrec def go(headElem: A): Unit = if (seq.hasNext) {
      val headSeq = seq.next()
      by(headElem, headSeq) match {
        case Some(c) =>
          zipped += c
          if (elems.hasNext) go(elems.next())
        case None => go(headElem)
      }
    }

    if (elems.hasNext) go(elems.next())
    zipped.result()
  }

  /** @throws java.lang.IllegalArgumentException If `objects` contains more than one distinct element */
  def assertAtMostOne[T](objects: Seq[T], objName: String)(implicit
      loggingContext: ErrorLoggingContext
  ): Option[T] =
    objects.distinct match {
      case Seq() => None
      case Seq(single) => Some(single)
      case multiples =>
        ErrorUtil.internalError(
          new IllegalArgumentException(s"Multiple ${objName}s $multiples. Expected at most one.")
        )
    }

  /** Map the function `f` over a sequence and reduce the result with function `g`,
    * mapping and reducing is done in parallel given the desired `parallelism`.
    *
    * This method works best if the amount of work for computing `f` and `g` is roughly constant-time,
    * i.e., independent of the data that is being processed, because then each chunk to process takes about
    * the same time.
    *
    * @param parallelism Determines the number of chunks that are created for parallel processing.
    * @param f The mapping function.
    * @param g The reducing function. Must be associative.
    * @return The result of `xs.map(f).reduceOption(g)`. If `f` or `g` throw exceptions,
    *         the returned future contains such an exception, but it is not guaranteed
    *         that the returned exception is the "first" such exception in a fixed sequential execution order.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  def mapReducePar[A, B](parallelism: PositiveNumeric[Int], xs: Seq[A])(
      f: A => B
  )(g: (B, B) => B)(implicit ec: ExecutionContext): Future[Option[B]] = {
    if (xs.isEmpty) { Future.successful(None) }
    else {
      val futureCount = parallelism.value
      val chunkSize = (xs.size + futureCount - 1) / futureCount
      Future
        .traverse(xs.grouped(chunkSize)) { chunk =>
          // Run map-reduce in parallel for each chunk
          Future {
            val mapped = chunk.map(f)
            checked(mapped.reduce(g)) // `chunk` is non-empty
          }
        }
        .map { reducedChunks =>
          // Finally reduce the reducts of the chunks
          Some(reducedChunks.reduce(g))
        }
    }
  }

  /** Calculates the largest possible list ys of elements in an input iterable xs such that:
    * For all y in ys. y >= x for all x in xs.
    *
    * Informally, this gives the list of all highest elements of `xs`.
    *
    * See `TraversableUtilTest` for an example.
    */
  def maxList[A](xs: Iterable[A])(implicit order: Ordering[A]): List[A] = {
    xs.foldLeft(List.empty: List[A]) {
      case (Nil, x) => List(x)
      case (accs @ (acc :: _), x) =>
        val diff = order.compare(acc, x)
        if (diff < 0) List(x)
        else if (diff == 0)
          x :: accs
        else accs
    }
  }

  /** Calculates the largest possible list ys of elements in an input iterable xs such that:
    * For all y in ys. y <= x for all x in xs.
    *
    * Informally, this gives the list of all lowest elements of `xs`.
    */
  def minList[A](xs: Iterable[A])(implicit order: Ordering[A]): List[A] = {
    maxList[A](xs)(order.reverse)
  }

  def zipAllOption[A, B](xs: Seq[A], ys: Iterable[B]): Seq[(Option[A], Option[B])] =
    xs.map(Some(_)).zipAll(ys.map(Some(_)), None, None)
}
