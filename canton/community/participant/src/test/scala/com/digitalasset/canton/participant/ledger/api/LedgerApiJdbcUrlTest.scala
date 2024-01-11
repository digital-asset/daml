// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CommunityDbConfig.*
import com.digitalasset.canton.config.DbParametersConfig
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.FailedToConfigureLedgerApiStorage
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class LedgerApiJdbcUrlTest extends AnyWordSpec with BaseTest {
  "fromConfig for real examples should" should {
    "support in-mem h2" in {
      val result = forH2("""
          |url = "jdbc:h2:mem:db1;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
          |user = "participant1"
          |password = "pwd"
          |driver = org.h2.Driver
          |""".stripMargin)

      result.value shouldBe "jdbc:h2:mem:db1;MODE=PostgreSQL;schema=ledger_api;DB_CLOSE_DELAY=-1;user=participant1;password=pwd"
    }

    "support file backed h2" in {
      val result = forH2("""
          |connectionPool = disabled
          |url = "jdbc:h2:file:./participant2;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
          |user = "participant2"
          |password = "morethansafe"
          |driver = org.h2.Driver
          |""".stripMargin)

      result.value shouldBe "jdbc:h2:file:./participant2;MODE=PostgreSQL;schema=ledger_api;DB_CLOSE_DELAY=-1;user=participant2;password=morethansafe"
    }

    "support postgres datasource configuration" in {
      val result = forPostgres("""
          |dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
          |properties = {
          |  serverName = "localhost"
          |  portNumber = "5432"
          |  databaseName = "participant2"
          |  user = "participant2"
          |  password = "supersafe"
          |}
          |numThreads = 10
          |""".stripMargin)

      result.value shouldBe "jdbc:postgresql://localhost:5432/participant2?user=participant2&password=supersafe&currentSchema=ledger_api"
    }

    "propagate all properties" in {
      val result = forPostgres("""
                                 |dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
                                 |properties = {
                                 |  serverName = "localhost"
                                 |  portNumber = "5432"
                                 |  databaseName = "participant2"
                                 |  user = "participant2"
                                 |  password = "supersafe"
                                 |  ssl = "true"
                                 |  sslmode = "verify-ca"
                                 |  sslfactory = "org.postgresql.ssl.jdbc4.LibPQFactory"
                                 |  sslpassword = "evensafer"
                                 |  sslcert = "path/to/certificate.crt"
                                 |  sslrootcert = "path/to/root_certificate.crt"
                                 |  sslkey = "path/to/key.pk8"
                                 |  currentSchema = "participant_schema"
                                 |}
                                 |numThreads = 10
                                 |""".stripMargin)

      result.value shouldBe "jdbc:postgresql://localhost:5432/participant2?" +
        "sslrootcert=path%2Fto%2Froot_certificate.crt&" +
        "sslpassword=evensafer&" +
        "sslkey=path%2Fto%2Fkey.pk8&" +
        "sslcert=path%2Fto%2Fcertificate.crt&" +
        "sslmode=verify-ca&" +
        "sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory&" +
        "user=participant2&" +
        "password=supersafe&" +
        "ssl=true&" +
        "currentSchema=ledger_api"
    }

    "support postgres jdbc driver configuration" in {
      val result = forPostgres("""
          |url = "jdbc:postgresql://0.0.0.0:5432/participant2"
          |user = "participant2"
          |password = "supersafe"
          |driver = org.postgresql.Driver
          |""".stripMargin)
      result.value shouldBe "jdbc:postgresql://0.0.0.0:5432/participant2?user=participant2&password=supersafe&currentSchema=ledger_api"
    }

    "global domain config" in {
      val result = forPostgres("""
          |dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
          |properties = {
          |  serverName = db.canton.global
          |  portNumber = 1234
          |  user = long-user-name
          |  password = some-password
          |}
          |connectionPool = HikariCP
          |maxConnections = 8
          |maxThreads = 8
          |numThreads = 8
          |""".stripMargin)

      result.value shouldBe "jdbc:postgresql://db.canton.global:1234/?user=long-user-name&password=some-password&currentSchema=ledger_api"
    }

    "generated azure connection string" in {
      val result = forPostgres("""
          |url = "jdbc:postgresql://my-db.postgres.database.azure.com:5432/canton?user=canton@canton-db&password=abcABC123&sslmode=require"
          |""".stripMargin)

      result.value shouldBe "jdbc:postgresql://my-db.postgres.database.azure.com:5432/canton?user=canton@canton-db&password=abcABC123&sslmode=require&currentSchema=ledger_api"
    }
  }

  "special characters" should {
    "be properly encoded" in {
      val result = forPostgres("""
         |dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
         |properties = {
         |  serverName = db.canton.global
         |  portNumber = 1234
         |  user = "you@can%&really&do!stupid@stuff3"
         |  password = "AAAA&#@!/:=üöéè +AAAA"
         |}
         |connectionPool = HikariCP
         |""".stripMargin)

      result.value shouldBe "jdbc:postgresql://db.canton.global:1234/?user=you%40can%25%26really%26do%21stupid%40stuff3&password=AAAA%26%23%40%21%2F%3A%3D%C3%BC%C3%B6%C3%A9%C3%A8+%2BAAAA&currentSchema=ledger_api"
    }
  }

  "incorrect config" should {
    "return a decent error if a postgres configuration is missing enough to generate a url" in {
      val result = forPostgres("""
          |properties = {
          |  portNumber = 1234
          |}
          |""".stripMargin)

      result.left.value shouldBe FailedToConfigureLedgerApiStorage(
        "Could not generate a postgres jdbc url from the specified fields: [port]"
      )
    }
  }

  "schema specification" should {
    "replace the schema if specified for h2" in {
      val result = forH2("""
          |url = "jdbc:h2:mem:db1;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;SCHEMA=another-schema"
          |user = "participant1"
          |password = "pwd"
          |""".stripMargin)

      result.value shouldBe "jdbc:h2:mem:db1;MODE=PostgreSQL;schema=ledger_api;DB_CLOSE_DELAY=-1;user=participant1;password=pwd"
    }

    "replace the schema if specified for postgres" in {
      val result = forPostgres("""
          |url = "jdbc:postgresql://host/canton?user=user&currentSchema=my-schema&password=pwd&sslmode=require"
          |""".stripMargin)

      result.value shouldBe "jdbc:postgresql://host/canton?user=user&password=pwd&sslmode=require&currentSchema=ledger_api"
    }
  }

  "specifying an explicit ledger-api url" should {
    "use that rather than generating one" in {
      val dbConfig = H2(
        ConfigFactory.empty(),
        DbParametersConfig(ledgerApiJdbcUrl = Some("use-this-jdbc-url-please")),
      )
      LedgerApiJdbcUrl.fromDbConfig(dbConfig).value.url shouldBe "use-this-jdbc-url-please"
    }
  }

  private def forH2(configText: String): Either[FailedToConfigureLedgerApiStorage, String] =
    LedgerApiJdbcUrl.fromDbConfig(H2(ConfigFactory.parseString(configText))).map(_.url)

  private def forPostgres(configText: String): Either[FailedToConfigureLedgerApiStorage, String] =
    LedgerApiJdbcUrl.fromDbConfig(Postgres(ConfigFactory.parseString(configText))).map(_.url)
}
