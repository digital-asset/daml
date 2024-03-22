// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.resources

import org.scalatest.AsyncTestSuite

trait TestResourceContext {
  self: AsyncTestSuite =>

  protected implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
}
