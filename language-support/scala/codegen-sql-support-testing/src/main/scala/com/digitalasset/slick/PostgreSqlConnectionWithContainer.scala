// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.slick

import org.testcontainers.containers.PostgreSQLContainer
import slick.jdbc.PostgresProfile

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.concurrent.duration._
import scala.language.existentials

import com.digitalasset.util.retryingAfter

/**
  * PostgreSQL Slick connection with a PostgreSQL docker container.
  * == WARNING ==
  * Not for production code, for test purposes only.
  * Both docker container and PostgreSQL connection can throw exceptions.
  */
class PostgreSqlConnectionWithContainer extends SlickConnection[PostgresProfile] {

  private val container = postgreSqlDockerContainer()

  private val connection = postgreSqlConnection(container, maxAttempts = 5)

  override val profile: PostgresProfile = connection.profile

  override val db: PostgresProfile.api.Database = connection.db

  // attempt to close all, throw first occurred exception else return Unit
  def close(): Unit = {
    import com.digitalasset.util.TryOps.sequence
    sequence(
      List(
        Try(connection.close()),
        Try(container.close())
      )) match {
      case Failure(e) => throw e
      case Success(_) => ()
    }
  }

  private def postgreSqlDockerContainer(): PostgreSQLContainer[_] =
    try {
      retryingAfter(5.seconds, 30.seconds, 1.minute, 5.minutes) { () =>
        val container = new PostgreSQLContainer
        container.start()
        container
      }
    } catch {
      case NonFatal(e) =>
        throw CannotStartDockerContainer(classOf[PostgreSQLContainer[_]].getSimpleName, e)
    }

  def postgreSqlConnection(
      container: PostgreSQLContainer[_],
      maxAttempts: Int): PostgreSqlConnection = {
    require(maxAttempts >= 1)
    try {
      new PostgreSqlConnection(
        jdbcUrl = container.getJdbcUrl,
        username = container.getUsername,
        password = container.getPassword)
    } catch {
      case NonFatal(_) if maxAttempts > 1 =>
        Thread.sleep(1.second.toMillis)
        postgreSqlConnection(container, maxAttempts - 1)
    }
  }
}

case class CannotStartDockerContainer(container: String, cause: Throwable)
    extends IllegalStateException(
      s"Cannot start docker container: $container. Check the cause of this exception for details (is the docker daemon up?)",
      cause)
