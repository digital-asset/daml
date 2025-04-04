// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.logging

import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell

trait LoggingIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(1, 0, 0)
      .withManualStart

  private val bigTraceId = new SingleUseCell[String]()

  "logging.last_errors return last logged errors" in { implicit env =>
    import env.*

    participant1.start()

    def traceId(implicit traceContext: TraceContext): String =
      traceContext.traceId.valueOrFail("trace-id not set")

    TraceContext.withNewTraceContext { implicit traceContext =>
      bigTraceId.putIfAbsent(traceId)
      val infoMsg = "first info message"
      logger.info(infoMsg)
      val warnMsg1 = "first warn message"
      logger.warn(warnMsg1)
      val errorMsg1 = "first error message"
      logger.error(errorMsg1)
      val errorMsg2 = "second error message"
      logger.error(errorMsg2)

      val warnMsg2 = "warn message from another trace-context"

      val traceId2 = TraceContext.withNewTraceContext { implicit traceContext =>
        logger.warn(warnMsg2)
        traceId
      }

      val lastErrors = logging.last_errors()
      Seq(traceId, traceId2).foreach(lastErrors should contain key _)

      val firstError = logging.last_error_trace(traceId)
      Seq(infoMsg, warnMsg1, errorMsg1, errorMsg2).forall { msg =>
        firstError.exists(_.contains(msg))
      }
    }
  }

  "last errors can be accessed remotely" in { implicit env =>
    import env.*

    // i can access last errors remotely
    val errs = participant1.health.last_errors()
    errs should not be empty

    // i can access the trace of the last error remotely
    participant1.health.last_error_trace(
      bigTraceId.getOrElse(fail("where is my trace-id?"))
    ) should not be empty

  }

}

class LoggingIntegrationTestDefault extends LoggingIntegrationTest
