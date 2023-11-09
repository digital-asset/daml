// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.perf.util

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.api.testing.utils.Resource
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

@SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
abstract class AkkaStreamPerformanceTest extends PerformanceTest {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  type ResourceType

  @volatile protected var system: ActorSystem = _
  @volatile protected var materializer: Materializer = _
  @transient protected implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  protected def resource: Resource[ResourceType]

  protected def setup(): Unit = {
    resource.setup()
    implicit val sys: ActorSystem = ActorSystem(this.getClass.getSimpleName.stripSuffix("$"))
    system = sys
    materializer = Materializer(system)
  }

  protected def teardown(): Unit = {
    await(system.terminate())
    resource.close()
  }

  implicit class FixtureSetup[T](using: Using[T]) extends Serializable {
    def withLifecycleManagement(additionalSetup: T => Unit = _ => ()): Using[T] =
      using
        .setUp { input =>
          try {
            setup()
            additionalSetup(input)
          } catch {
            case t: Throwable =>
              logger.error("Setup failed.", t)
              throw t
          }
        }
        .tearDown { _ =>
          try {
            teardown()
          } catch {
            case t: Throwable =>
              logger.error("Teardown failed.", t)
              throw t
          }
        }
  }
}
