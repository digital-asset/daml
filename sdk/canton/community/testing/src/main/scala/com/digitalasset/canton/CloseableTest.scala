// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait CloseableTest extends BeforeAndAfterAll with FlagCloseable with HasCloseContext {
  self: Suite =>

  override def afterAll(): Unit = {
    close()
    // Ensure "sibling traits" such as EnvironmentSetup clean up as well where afterAll closes plugins
    // including docker test containers (#13786, for an overview of how scala linearizes traits, see
    // https://blog.knoldus.com/basics-of-trait-stackable-modifications-linearlization-in-scala).
    super.afterAll()
  }

}
