// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.slick
import slick.jdbc.JdbcProfile

trait SlickConnection[Profile <: JdbcProfile] {
  val profile: Profile
  val db: profile.api.Database
  def close(): Unit
}

class GenericSlickConnection[Profile <: JdbcProfile](
    override val profile: Profile,
    driver: String,
    url: String,
    user: Option[String] = None,
    password: Option[String] = None,
    maxThreads: Int = 1,
    queueSize: Int = 1024)
    extends SlickConnection[Profile] {

  override val db: profile.api.Database = connect()

  private def connect(): profile.api.Database = {
    profile.api.Database.forURL(
      url = url,
      driver = driver,
      user = user.orNull,
      password = password.orNull,
      executor = profile.api.AsyncExecutor("slick", maxThreads, queueSize),
      keepAliveConnection = true
    )
  }

  override def close(): Unit = {
    db.close()
  }
}
