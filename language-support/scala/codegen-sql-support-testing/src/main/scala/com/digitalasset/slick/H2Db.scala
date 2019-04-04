// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.slick

import com.digitalasset.slick.H2Db.uniqueId

/**
  * Connection to in-memory H2 DB instance.
  */
class H2Db
    extends GenericSlickConnection(
      profile = slick.jdbc.H2Profile,
      driver = "org.h2.Driver",
      url = s"jdbc:h2:mem:$uniqueId;DB_CLOSE_DELAY=-1")

object H2Db {
  def uniqueId: String = java.util.UUID.randomUUID().toString
}
