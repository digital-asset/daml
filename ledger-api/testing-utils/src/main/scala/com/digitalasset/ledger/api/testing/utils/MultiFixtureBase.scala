// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.daml.logging.LoggingContext
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScaledTimeSpans}
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.time.Span
import org.scalatest.{Assertion, Assertions, AsyncTestSuite, BeforeAndAfterAll, Succeeded}

import scala.collection.immutable.Iterable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.util.control.{NoStackTrace, NonFatal}

trait MultiFixtureBase[FixtureId, TestContext]
    extends Assertions
    with BeforeAndAfterAll
    with ScaledTimeSpans
    with AsyncTimeLimitedTests {
  self: AsyncTestSuite =>

  protected implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private var es: ScheduledExecutorService = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    es = Executors.newScheduledThreadPool(1)
  }

  override protected def afterAll(): Unit = {
    es.shutdownNow()
    super.afterAll()
  }

  protected class TestFixture(val id: FixtureId, createContext: () => TestContext) {
    def context(): TestContext = createContext()
  }

  def timeLimit: Span = scaled(30.seconds)

  object TestFixture {
    def apply(id: FixtureId, createContext: () => TestContext): TestFixture =
      new TestFixture(id, createContext)

    def unapply(testFixture: TestFixture): Option[(FixtureId, TestContext)] =
      Some((testFixture.id, testFixture.context()))
  }

  protected def fixtures: Iterable[TestFixture]

  /** If true, each test case will be ran against all fixtures in parallel.
    * If false, execution will happen sequentially.
    */
  protected def parallelExecution: Boolean = true

  /*
  This is just here to be extra prudent, as Succeeded should be the only instance of Assertion,
  and failures are communicated through exceptions in ScalaTest.
   */
  private def foldAssertions(as: Iterable[Assertion]): Assertion =
    as.foldLeft(Succeeded: Assertion) { (acc, newResult) =>
      if (acc == Succeeded && newResult == Succeeded) Succeeded
      else newResult
    }

  private def runTestAgainstFixture(
      testFixture: TestFixture,
      runTest: TestFixture => Future[Assertion],
  ): Future[Assertion] = {

    def failOnFixture(throwable: Throwable): Assertion = {
      // Add additional information about failure (which fixture was problematic)
      throwable match {
        case ex: TestCanceledException => throw ex
        case _ =>
          throw new RuntimeException(
            s"Test failed on fixture ${testFixture.id} with ${throwable.getClass}: ${throwable.getMessage}",
            throwable,
          ) with NoStackTrace
      }
    }

    val timeoutPromise = Promise[Assertion]()
    es.schedule(
      () => timeoutPromise.failure(new TimeoutException(s"Timed out after $timeLimit")),
      timeLimit.toMillis,
      TimeUnit.MILLISECONDS,
    )

    try {
      Future
        .firstCompletedOf(List(runTest(testFixture), timeoutPromise.future))(
          ExecutionContext.parasitic
        )
        .recover { case NonFatal(throwable) =>
          failOnFixture(throwable)
        }(ExecutionContext.parasitic)
    } catch {
      case NonFatal(throwable) => failOnFixture(throwable)
    }
  }

  /** Same as forAllFixtures, nicer to use with the "test" in allFixtures { ctx => ??? } syntax */
  protected def allFixtures(runTest: TestContext => Future[Assertion]): Future[Assertion] =
    forAllFixtures(fixture => runTest(fixture.context()))

  protected def forAllFixtures(runTest: TestFixture => Future[Assertion]): Future[Assertion] = {
    forAllMatchingFixtures { case f => runTest(f) }
  }

  protected def forAllMatchingFixtures(
      runTest: PartialFunction[TestFixture, Future[Assertion]]
  ): Future[Assertion] = {
    if (parallelExecution) {
      val results = fixtures.map(fixture =>
        if (runTest.isDefinedAt(fixture))
          runTestAgainstFixture(fixture, runTest)
        else
          Future.successful(succeed)
      )
      Future.sequence(results).map(foldAssertions)
    } else {
      fixtures.foldLeft(Future.successful(succeed)) { case (resultSoFar, thisFixture) =>
        resultSoFar.flatMap {
          case Succeeded => runTestAgainstFixture(thisFixture, runTest)
          case other => Future.successful(other)
        }
      }
    }
  }

}
