// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SuiteResource[+T] {
  protected def suiteResource: Resource[T]
}

/** Serves only to enforce the final mixin of a SuiteResourceManagementAroundAll or a SuiteResourceManagementAroundEach */
trait SuiteResourceManagement {}

trait SuiteResourceManagementAroundAll
    extends SuiteResource[Any]
    with SuiteResourceManagement
    with BeforeAndAfterAll {
  self: Suite =>

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    suiteResource.setup()
  }

  override protected def afterAll(): Unit = {
    suiteResource.close()
    super.afterAll()
  }
}

trait SuiteResourceManagementAroundEach
    extends SuiteResource[Any]
    with SuiteResourceManagement
    with BeforeAndAfterEach {
  self: Suite =>

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    suiteResource.setup()
  }

  override protected def afterEach(): Unit = {
    suiteResource.close()
    super.afterEach()
  }
}
