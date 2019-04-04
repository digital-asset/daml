// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

import org.scalatest.{BeforeAndAfterAll, Suite}
import slick.jdbc.H2Profile.api._

trait H2TestUtils extends BeforeAndAfterAll { this: Suite =>
  lazy val unique = java.util.UUID.randomUUID().toString
  lazy val db = Database.forURL(
    s"jdbc:h2:mem:$unique;DB_CLOSE_DELAY=-1",
    driver = "org.h2.Driver",
    executor = AsyncExecutor("bla", 1, -1))

  override protected def beforeAll(): Unit = super.beforeAll()

  override protected def afterAll(): Unit = {
    super.afterAll()
    db.close()
  }
}
