// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, Suite}

trait TestAppenderFixture extends BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  self: Suite =>
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestAppender.enable()
  }
  override protected def afterAll(): Unit = {
    TestAppender.disable()
    super.afterAll()
  }
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    TestAppender.isEnabled shouldBe true
    ()
  }
}
