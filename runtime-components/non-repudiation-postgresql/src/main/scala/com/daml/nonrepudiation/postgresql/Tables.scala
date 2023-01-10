// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import java.util.Collections

import cats.effect.IO
import doobie.hikari.HikariTransactor
import doobie.util.log.LogHandler
import org.flywaydb.core.Flyway

object Tables {

  val Prefix = "nonrepudiation"

  def initialize(transactor: HikariTransactor[IO])(implicit logHandler: LogHandler): Tables =
    transactor
      .configure { dataSource =>
        IO {
          Flyway
            .configure()
            .dataSource(dataSource)
            .locations("classpath:com/daml/nonrepudiation/postgresql/")
            .placeholders(Collections.singletonMap("tables.prefix", Prefix))
            .table(s"${Prefix}_schema_history")
            .load()
            .migrate()
          new Tables(
            certificates = new PostgresqlCertificateRepository(transactor),
            signedPayloads = new PostgresqlSignedPayloadRepository(transactor),
          )
        }
      }
      .unsafeRunSync()

}

final class Tables private (
    val certificates: PostgresqlCertificateRepository,
    val signedPayloads: PostgresqlSignedPayloadRepository,
)
