// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.implicits.*
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import org.scalatest.*

import scala.annotation.tailrec
import scala.concurrent.Future

/** A trait containing the tag to enable repeat runs of tests.
  */
sealed trait RepeatableTest extends NamedLogging {

  /** Marker to trigger repeated runs of classes extending [[com.digitalasset.canton.RepeatableTest]],
    * running up to n times or until failure.
    * @param n Maximum number of runs of the test.
    */
  case class RepeatTest(n: Int) extends Tag(s"Repeated ${n}")

}

object RepeatableTest {
  def repeats(tags: Set[String]): Int = {
    val tag = tags.find(_.contains("Repeated "))
    tag.map(_.drop("Repeated ".length).toInt).getOrElse(1)
  }
}

trait RepeatableTestSuiteTest extends RepeatableTest with TestSuite {
  protected implicit def traceContext: TraceContext

  /** This override enables repeated runs of tests in suites extending [[org.scalatest.TestSuite]]
    */
  override def withFixture(test: NoArgTest): Outcome = {
    val repeat = RepeatableTest.repeats(test.tags)
    @tailrec def go(iter: Int): Outcome = {
      if (repeat > 1) logger.info(s"Iteration $iter for sync test")
      val result = super.withFixture(test)
      val next = iter + 1
      if (!result.isSucceeded || next >= repeat) result
      else go(next)
    }

    if (repeat > 1) logger.info(s"Repeating sync test $repeat times")
    go(0)
  }
}

trait RepeatableAsyncTestSuiteTest extends RepeatableTest with AsyncTestSuite {

  /** This override enables repeated runs of tests in suites extending [[org.scalatest.TestSuite]]
    */
  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    import TraceContext.Implicits.Empty.*

    val repeat = RepeatableTest.repeats(test.tags)
    if (repeat > 1) logger.info(s"Repeating async test $repeat times")
    val init = super.withFixture(test).toFuture

    def process(outcome: Outcome): Future[Outcome] = {
      if (outcome.isSucceeded)
        // Run the test again
        super.withFixture(test).toFuture
      else Future.successful(outcome)
    }
    val result = MonadUtil.repeatFlatmap(init, process, repeat - 1);
    new FutureOutcome(result)
  }

}
