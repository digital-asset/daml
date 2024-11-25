// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.util.PekkoUtil
import org.apache.pekko.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Mixin to provide an implicit [[org.apache.pekko.actor.ActorSystem]] to a test suite */
trait HasActorSystem extends BeforeAndAfterAll {
  this: Suite with HasExecutionContext with NamedLogging =>

  protected implicit lazy val actorSystem: ActorSystem =
    PekkoUtil.createActorSystem(getClass.getSimpleName)

  protected def timeouts: ProcessingTimeout

  override def afterAll(): Unit =
    try LifeCycle.close(LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts))(logger)
    finally super.afterAll()
}
