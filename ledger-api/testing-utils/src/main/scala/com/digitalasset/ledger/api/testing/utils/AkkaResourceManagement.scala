// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.scalatest.Suite

trait AkkaResourceManagement extends SuiteResource[Materializer] {
  self: Suite =>

  override protected lazy val suiteResource: Resource[Materializer] =
    ActorMaterializerResource(actorSystemName)

  protected def actorSystemName: String = {
    this.getClass.getSimpleName.stripSuffix("$")
  }

  implicit protected def system: ActorSystem = suiteResource.value.system

  implicit protected def materializer: Materializer = suiteResource.value
}
