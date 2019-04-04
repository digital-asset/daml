// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.Suite

trait AkkaResourceManagement extends SuiteResource[ActorMaterializer] {
  self: Suite =>

  override protected lazy val suiteResource: Resource[ActorMaterializer] =
    ActorMaterializerResource(actorSystemName)

  protected def actorSystemName: String = {
    this.getClass.getSimpleName.stripSuffix("$")
  }

  implicit protected def system: ActorSystem = suiteResource.value.system

  implicit protected def materializer: ActorMaterializer = suiteResource.value
}
