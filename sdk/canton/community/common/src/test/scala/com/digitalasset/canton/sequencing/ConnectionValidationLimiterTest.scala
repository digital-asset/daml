// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.parallel.*
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger

class ConnectionValidationLimiterTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "ConnectionValidationLimiter" should {
    "limit to 2 validations in the presence of a burst" in {
      val promises = Seq.fill(2)(PromiseUnlessShutdown.unsupervised[Unit]())
      val counter = new AtomicInteger(0)

      def mockValidate(traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        val myIndex = counter.getAndIncrement()
        logger.debug(s"Running validation #$myIndex")(traceContext)
        promises(myIndex).futureUS
      }

      val validator =
        new ConnectionValidationLimiter(mockValidate, futureSupervisor, loggerFactory)

      // Request a burst of validations
      val fut = (1 to 42).toList.map(_ => validator.maybeValidate()(TraceContext.createNew()))

      // Complete all validations
      promises.foreach(_.outcome_(()))

      // Wait for all validation requests to complete
      fut.parSequence.futureValueUS

      // We should have 2 validations
      eventually()(counter.get shouldBe 2)
    }

    "shut down the validations when shutdown" in {
      val promises = Seq(PromiseUnlessShutdown.unsupervised[Unit]())
      val counter = new AtomicInteger(0)

      def mockValidate(traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        val myIndex = counter.getAndIncrement()
        logger.debug(s"Running validation #$myIndex")(traceContext)
        promises(myIndex).futureUS
      }

      val validator =
        new ConnectionValidationLimiter(mockValidate, futureSupervisor, loggerFactory)

      // Request two validations so one gets scheduled
      val fut = (1 to 2).toList.map(_ => validator.maybeValidate()(TraceContext.createNew()))

      // Shutdown the validator
      validator.close()

      // All validation requests should be shutdown
      forAll(fut.map(_.unwrap.futureValue))(_ shouldBe UnlessShutdown.AbortedDueToShutdown)

      // Only one validation has run
      eventually()(counter.get shouldBe 1)
    }

    "shut down a scheduled validation when the running validation is shutdown" in {
      val promises = Seq(PromiseUnlessShutdown.unsupervised[Unit]())
      val counter = new AtomicInteger(0)

      def mockValidate(traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        val myIndex = counter.getAndIncrement()
        logger.debug(s"Running validation #$myIndex")(traceContext)
        promises(myIndex).futureUS
      }

      val validator =
        new ConnectionValidationLimiter(mockValidate, futureSupervisor, loggerFactory)

      // Request two validations so one gets scheduled
      val fut = (1 to 2).toList.map(_ => validator.maybeValidate()(TraceContext.createNew()))

      // Shutdown the first validation
      promises(0).shutdown_()

      // All validation requests should be shutdown
      forAll(fut.map(_.unwrap.futureValue))(_ shouldBe UnlessShutdown.AbortedDueToShutdown)

      // Only one validation has run
      eventually()(counter.get shouldBe 1)
    }

    "fails a scheduled validation when a validation throws" in {
      val promises = Seq(PromiseUnlessShutdown.unsupervised[Unit]())
      val counter = new AtomicInteger(0)

      def mockValidate(traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        val myIndex = counter.getAndIncrement()
        logger.debug(s"Running validation #$myIndex")(traceContext)
        promises(myIndex).futureUS
      }

      val validator =
        new ConnectionValidationLimiter(mockValidate, futureSupervisor, loggerFactory)

      // Request two validations so one gets scheduled
      val fut = (1 to 2).toList.map(_ => validator.maybeValidate()(TraceContext.createNew()))

      // Fail the first validation
      promises(0).failure(new Exception("boom"))

      // All validation requests should fail
      forAll(fut.map(_.unwrap.failed.futureValue))(_.getMessage should include("boom"))

      // Only one validation has run
      eventually()(counter.get shouldBe 1)
    }
  }
}
