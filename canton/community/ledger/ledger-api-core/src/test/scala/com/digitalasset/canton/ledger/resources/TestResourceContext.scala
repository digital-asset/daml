// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.resources

import com.daml.ledger.resources.ResourceContext
import org.scalatest.AsyncTestSuite

trait TestResourceContext {
  self: AsyncTestSuite =>

  protected implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
}
