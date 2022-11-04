// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.stream.ThrottleMode
import com.daml.bazeltools.BazelRunfiles.requiredResource
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.dbutils
import com.daml.dbutils.{JdbcConfig => DbUtilsJdbcConfig}
import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.cliopts.Logging.LogEncoder
import com.daml.http.dbbackend.{DbStartupMode, JdbcConfig}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authentication
import com.daml.test.evidence.tag.Security.SecurityTest
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import java.io.File
import java.nio.file.Paths
import com.daml.metrics.api.reporters.MetricsReporter
import scala.concurrent.duration._

object CliSpec {
  private val poolSize = 10
  private val minIdle = 4
  private val connectionTimeout = 5000.millis
  private val idleTimeout = 10000.millis
  private val tablePrefix = "foo"
}
final class CliSpec extends AnyFreeSpec with Matchers {
  import CliSpec._

  private def configParser(
      parameters: Seq[String],
      getEnvVar: String => Option[String] = (_ => None),
  ): Option[Config] =
    Cli.parseConfig(parameters, Set("org.postgresql.Driver"), getEnvVar)

  private val authenticationSecurity = SecurityTest(property = Authentication, asset = "TBD")

  val jdbcConfig = JdbcConfig(
    dbutils.JdbcConfig(
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/test?&ssl=true",
      "postgres",
      "password",
      poolSize,
      minIdle,
      connectionTimeout,
      idleTimeout,
      tablePrefix,
    ),
    startMode = DbStartupMode.StartOnly,
  )
  val jdbcConfigString =
    "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password," +
      s"poolSize=$poolSize,minIdle=$minIdle,connectionTimeout=${connectionTimeout.toMillis}," +
      s"idleTimeout=${idleTimeout.toMillis},tablePrefix=$tablePrefix"

  val sharedOptions =
    Seq("--ledger-host", "localhost", "--ledger-port", "6865", "--http-port", "7500")

  "LogLevel" - {
    import ch.qos.logback.classic.{Level => LogLevel}

    def logLevelArgs(level: String) = Seq("--log-level", level)

    def checkLogLevelWorks(level: String, expected: LogLevel) = {
      val config = configParser(logLevelArgs(level) ++ sharedOptions)
        .getOrElse(fail())
      config.logLevel shouldBe Some(expected)
    }

    "should get the error log level from the command line argument when provided" in {
      checkLogLevelWorks("error", LogLevel.ERROR)
    }

    "should get the warn log level from the command line argument when provided" in {
      checkLogLevelWorks("warn", LogLevel.WARN)
    }

    "should get the info log level from the command line argument when provided" in {
      checkLogLevelWorks("info", LogLevel.INFO)
    }

    "should get the debug log level from the command line argument when provided" in {
      checkLogLevelWorks("debug", LogLevel.DEBUG)
    }

    "should get the trace log level from the command line argument when provided" in {
      checkLogLevelWorks("trace", LogLevel.TRACE)
    }

    "shouldn't get a config parser result if an invalid log level is provided via a command line argument" in {
      val config = configParser(logLevelArgs("SUPERFANCYLOGLEVEL") ++ sharedOptions)
      config shouldBe None
    }

    "should get a config parser result if no log level is provided via a command line argument" in {
      val config = configParser(sharedOptions).getOrElse(fail())
      config.logLevel shouldBe None
    }
  }

  "JdbcConfig" - {
    "should get the jdbc string from the command line argument when provided" in {
      val config = configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
        .getOrElse(fail())
      config.jdbcConfig shouldBe Some(jdbcConfig)
    }

    "should get the jdbc string from the environment when provided" in {
      val jdbcEnvVar = "JDBC_ENV_VAR"
      val config = configParser(
        Seq("--query-store-jdbc-config-env", jdbcEnvVar) ++ sharedOptions,
        { case `jdbcEnvVar` => Some(jdbcConfigString) },
      ).getOrElse(fail())
      config.jdbcConfig shouldBe Some(jdbcConfig)
    }

    "should get None when neither CLI nor env variable are provided" in {
      val config = configParser(sharedOptions).getOrElse(fail())
      config.jdbcConfig shouldBe None
    }

    "should get the table prefix if specified" in {
      val prefix = "some_fancy_prefix_"
      val config = configParser(
        Seq("--query-store-jdbc-config", s"$jdbcConfigString,tablePrefix=$prefix") ++ sharedOptions
      ).getOrElse(fail())
      config.jdbcConfig shouldBe Some(
        jdbcConfig.copy(baseConfig = jdbcConfig.baseConfig.copy(tablePrefix = prefix))
      )
    }

    "DbStartupMode" - {
      val jdbcConfigShared =
        "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password," +
          s"poolSize=$poolSize,minIdle=$minIdle,connectionTimeout=${connectionTimeout.toMillis}," +
          s"idleTimeout=${idleTimeout.toMillis},tablePrefix=$tablePrefix"

      "should get the CreateOnly startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=create-only"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(jdbcConfig.copy(startMode = DbStartupMode.CreateOnly))
      }

      "should get the StartOnly startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=start-only"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(jdbcConfig.copy(startMode = DbStartupMode.StartOnly))
      }

      "should get the CreateIfNeededAndStart startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=create-if-needed-and-start"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(startMode = DbStartupMode.CreateIfNeededAndStart)
        )
      }

      "should get the CreateAndStart startup mode from the string" in {
        val jdbcConfigString = s"$jdbcConfigShared,start-mode=create-and-start"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(startMode = DbStartupMode.CreateAndStart)
        )
      }

      "createSchema=false is converted to StartOnly" in {
        val jdbcConfigString = s"$jdbcConfigShared,createSchema=false"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(startMode = DbStartupMode.StartOnly)
        )
      }

      "createSchema=true is converted to CreateOnly" in {
        val jdbcConfigString = s"$jdbcConfigShared,createSchema=true"
        val config =
          configParser(Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions)
            .getOrElse(fail())
        config.jdbcConfig shouldBe Some(
          jdbcConfig.copy(startMode = DbStartupMode.CreateOnly)
        )
      }
    }

    "DisableContractPayloadIndexing" - {
      import dbbackend.Queries.Oracle
      import dbbackend.OracleQueries.DisableContractPayloadIndexing
      // we can always use postgres here; but if we integrate driver init with cmdline parse
      // we'll have to restrict this test block to EE-only
      val jdbcConfigShared =
        "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password"
      val expectedExtraConf = Map(DisableContractPayloadIndexing -> "true")

      "parses from command-line string" in {
        val jdbcConfigString = s"$jdbcConfigShared,$DisableContractPayloadIndexing=true"
        val config = configParser(
          Seq("--query-store-jdbc-config", jdbcConfigString) ++ sharedOptions
        ).getOrElse(fail())
        import org.scalatest.OptionValues._
        config.jdbcConfig.value.backendSpecificConf should ===(expectedExtraConf)
      }

      "then parses through backend" in {
        Oracle.parseConf(expectedExtraConf) should ===(Right(true: DisableContractPayloadIndexing))
      }

      "or defaults to false" in {
        Oracle.parseConf(Map.empty) should ===(Right(false: DisableContractPayloadIndexing))
      }

      "but fails on bad input" in {
        Oracle.parseConf(Map(DisableContractPayloadIndexing -> "haha")).isLeft should ===(true)
      }
    }
  }

  "TypeConfig app Conf" - {
    val confFile = "ledger-service/http-json/src/test/resources/http-json-api-minimal.conf"

    "should fail on missing ledgerHost and ledgerPort if no config file supplied" in {
      configParser(sharedOptions.drop(4)) should ===(None)
    }
    "should fail on missing httpPort and no config file is supplied" in {
      configParser(sharedOptions.take(4)) should ===(None)
    }

    "should successfully load a minimal config file" in {
      val cfg = configParser(Seq("--config", requiredResource(confFile).getAbsolutePath))
      cfg shouldBe Some(
        Config.Empty.copy(httpPort = 7500, ledgerHost = "127.0.0.1", ledgerPort = 6400)
      )
    }

    "should load a minimal config file along with logging opts from cli" in {
      val cfg = configParser(
        Seq(
          "--config",
          requiredResource(confFile).getAbsolutePath,
          "--log-level",
          "DEBUG",
          "--log-encoder",
          "json",
        )
      )
      cfg shouldBe Some(
        Config(
          httpPort = 7500,
          ledgerHost = "127.0.0.1",
          ledgerPort = 6400,
          logLevel = Some(LogLevel.DEBUG),
          logEncoder = LogEncoder.Json,
        )
      )
    }

    "should ignore cli args when config file and cli args both are supplied" in {
      configParser(
        Seq("--config", requiredResource(confFile).getAbsolutePath) ++ sharedOptions
      ) shouldBe Some(
        Config.Empty.copy(httpPort = 7500, ledgerHost = "127.0.0.1", ledgerPort = 6400)
      )
    }

    "should successfully load a complete config file" taggedAs authenticationSecurity in {
      val baseConfig = DbUtilsJdbcConfig(
        url = "jdbc:postgresql://localhost:5432/test?&ssl=true",
        driver = "org.postgresql.Driver",
        user = "postgres",
        password = "password",
        poolSize = 12,
        idleTimeout = 12.seconds,
        connectionTimeout = 90.seconds,
        tablePrefix = "foo",
        minIdle = 4,
      )
      val expectedWsConfig = WebsocketConfig(
        maxDuration = 180.minutes,
        throttleElem = 20,
        throttlePer = 1.second,
        maxBurst = 20,
        mode = ThrottleMode.Enforcing,
        heartbeatPeriod = 1.second,
      )
      val expectedConfig = Config(
        ledgerHost = "127.0.0.1",
        ledgerPort = 6400,
        address = "127.0.0.1",
        httpPort = 7500,
        portFile = Some(Paths.get("port-file")),
        tlsConfig = TlsConfiguration(
          enabled = true,
          Some(new File("cert-chain.crt")),
          Some(new File("pvt-key.pem")),
          Some(new File("root-ca.crt")),
        ),
        jdbcConfig = Some(JdbcConfig(baseConfig, DbStartupMode.StartOnly, Map("foo" -> "bar"))),
        staticContentConfig = Some(StaticContentConfig("static", new File("static-content-dir"))),
        metricsReporter = Some(MetricsReporter.Console),
        metricsReportingInterval = 30.seconds,
        wsConfig = Some(expectedWsConfig),
        surrogateTpIdCacheMaxEntries = Some(2000L),
        packageMaxInboundMessageSize = Some(StartSettings.DefaultMaxInboundMessageSize),
      )
      val confFile = "ledger-service/http-json/src/test/resources/http-json-api.conf"
      val cfg = configParser(Seq("--config", requiredResource(confFile).getAbsolutePath))
      cfg shouldBe Some(expectedConfig)
    }
  }

}
