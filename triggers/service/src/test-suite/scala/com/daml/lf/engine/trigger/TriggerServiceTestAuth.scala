// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.scalatest.Assertion
import org.scalactic.source

import scala.concurrent.Future

class TriggerServiceTestAuth
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestInMem
    with AbstractTriggerServiceTestAuthMiddleware {
  protected[this] override def inClaims(self: ItVerbString, testFn: Future[Assertion])(implicit
      pos: source.Position
  ) =
    self ignore testFn
}

class TriggerServiceTestAuthClaims
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestInMem
    with AbstractTriggerServiceTestAuthMiddleware {
  override protected[this] def oauth2YieldsUserTokens = false
}
