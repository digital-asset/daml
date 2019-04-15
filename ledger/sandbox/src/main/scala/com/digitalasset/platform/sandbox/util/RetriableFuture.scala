package com.digitalasset.platform.sandbox.util

import com.digitalasset.platform.common.util.DirectExecutionContext

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Future, Promise}

//TODO: test it
object RetriableFuture {

  implicit class FutureOps[T](f: => Future[T]) {

    /** Retries the async operation with exponential back off until the predicate becomes true.
      *
      * @param p              the predicate to be tested
      * @param schedule       a scheduler to time the retries
      * @param maxNoOfRetries if reached we give up on retrying
      * @param startingDelay  the initial delay when backing off
      * @return a Future[T] which should return only if the predicate is true, or if MaxNoOfRetries has been reached
      */
    def retryExponentiallyUntil(
        p: T => Boolean,
        schedule: (FiniteDuration, Runnable) => Unit,
        maxNoOfRetries: Int = 10,
        startingDelay: FiniteDuration = 25.milliseconds
    ): Future[T] = {

      assert(startingDelay > 0.milliseconds, "the starting delay must be greater than 0")

      def go(duration: FiniteDuration, noOfTries: Int): Future[T] = {
        f.flatMap {
          case t if (!p(t)) =>
            if (noOfTries > maxNoOfRetries) {
              Future.successful(t)
            } else {
              val p = Promise[T]
              schedule(
                duration,
                () =>
                  p.completeWith(
                    go(duration * 2, noOfTries + 1)
                ))
              p.future
            }

          case rest => Future.successful(rest)
        }(DirectExecutionContext)
      }

      go(startingDelay, 1)
    }
  }

}
