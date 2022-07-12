// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.lf.interpretation.Limits
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.ContractKeyUniquenessMode
import com.daml.lf.{VersionRange, language}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import org.scalacheck.Gen
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.{ConfigConvert, ConfigReader, ConfigSource, ConfigWriter}
import PureConfigReaderWriter._
import com.daml.ledger.api.tls.{SecretsUrl, TlsVersion}
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.{ApiServerConfig, AuthServiceConfig}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.config.MetricsConfig
import com.daml.platform.configuration.{
  CommandConfiguration,
  IndexServiceConfig,
  InitialLedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.daml.platform.usermanagement.UserManagementConfig
import com.typesafe.config.ConfigFactory
import pureconfig.error.ConfigReaderFailures

import java.net.InetSocketAddress
import java.nio.file.Path
import java.time.Duration
import scala.reflect.{ClassTag, classTag}

class PureConfigReaderWriterSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with EitherValues {

  def convert[T](converter: ConfigReader[T], str: String): Either[ConfigReaderFailures, T] = {
    val value = ConfigFactory.parseString(str)
    for {
      source <- ConfigSource.fromConfig(value).cursor()
      result <- converter.from(source)
    } yield result
  }

  def testReaderWriterIsomorphism[T: ClassTag: ConfigWriter: ConfigReader](
      generator: Gen[T],
      name: Option[String] = None,
  ): Unit = {
    name.getOrElse(classTag[T].toString) should "be isomorphic" in forAll(generator) {
      generatedValue =>
        val writer = implicitly[ConfigWriter[T]]
        val reader = implicitly[ConfigReader[T]]
        reader.from(writer.to(generatedValue)).value shouldBe generatedValue
    }
  }

  testReaderWriterIsomorphism(ArbitraryConfig.duration)
  testReaderWriterIsomorphism(ArbitraryConfig.versionRange)
  testReaderWriterIsomorphism(ArbitraryConfig.limits)
  testReaderWriterIsomorphism(ArbitraryConfig.contractKeyUniquenessMode)
  testReaderWriterIsomorphism(ArbitraryConfig.engineConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.metricsReporter)
  testReaderWriterIsomorphism(ArbitraryConfig.metricRegistryType)
  testReaderWriterIsomorphism(ArbitraryConfig.metricConfig)
  testReaderWriterIsomorphism(Gen.oneOf(TlsVersion.allVersions))
  testReaderWriterIsomorphism(ArbitraryConfig.tlsConfiguration)
  testReaderWriterIsomorphism(ArbitraryConfig.port)
  testReaderWriterIsomorphism(
    ArbitraryConfig.initialLedgerConfiguration,
    Some("InitialLedgerConfiguration"),
  )
  testReaderWriterIsomorphism(ArbitraryConfig.clientAuth)
  testReaderWriterIsomorphism(ArbitraryConfig.userManagementConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.partyConfiguration)
  testReaderWriterIsomorphism(ArbitraryConfig.connectionPoolConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.postgresDataSourceConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.dataSourceProperties)
  testReaderWriterIsomorphism(ArbitraryConfig.rateLimitingConfig, Some("RateLimitingConfig"))
  testReaderWriterIsomorphism(ArbitraryConfig.indexerConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.indexerStartupMode)
  testReaderWriterIsomorphism(ArbitraryConfig.commandConfiguration)
  testReaderWriterIsomorphism(ArbitraryConfig.apiServerConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.haConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.lfValueTranslationCache)
  testReaderWriterIsomorphism(ArbitraryConfig.indexServiceConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.participantConfig)
  testReaderWriterIsomorphism(ArbitraryConfig.config)

  behavior of "Duration"

  it should "read/write against predefined values" in {
    def compare(duration: Duration, expectedString: String): Assertion = {
      javaDurationWriter.to(duration) shouldBe fromAnyRef(expectedString)
      javaDurationReader.from(fromAnyRef(expectedString)).value shouldBe duration
    }
    compare(Duration.ofSeconds(0), "0 days")
    compare(Duration.ofSeconds(1), "1 second")
    compare(Duration.ofSeconds(30), "30 seconds")
    compare(Duration.ofHours(1), "3600 seconds")
  }

  behavior of "PureConfigReaderWriter VersionRange[LanguageVersion]"

  it should "read/write against predefined values" in {
    def compare(
        range: VersionRange[language.LanguageVersion],
        expectedString: String,
    ): Assertion = {
      versionRangeWriter.to(range) shouldBe fromAnyRef(expectedString)
      versionRangeReader.from(fromAnyRef(expectedString)).value shouldBe range
    }
    compare(LanguageVersion.DevVersions, "daml-lf-dev-mode-unsafe")
    compare(LanguageVersion.EarlyAccessVersions, "early-access")
    compare(LanguageVersion.LegacyVersions, "legacy")

    versionRangeWriter.to(LanguageVersion.StableVersions) shouldBe fromAnyRef("early-access")

    versionRangeReader
      .from(fromAnyRef("stable"))
      .value shouldBe LanguageVersion.StableVersions
  }

  behavior of "Limits"

  it should "support current defaults" in {
    val value = """
        |      choice-controllers = 2147483647
        |      choice-observers = 2147483647
        |      contract-observers = 2147483647
        |      contract-signatories = 2147483647
        |      transaction-input-contracts = 2147483647""".stripMargin
    convert(interpretationLimitsConvert, value).value shouldBe Limits()
  }

  it should "read/write against predefined values" in {
    val value =
      ConfigFactory.parseString(
        """
        |      choice-controllers = 123
        |      choice-observers = 234
        |      contract-observers = 345
        |      contract-signatories = 456
        |      transaction-input-contracts = 567
        |""".stripMargin
      )

    val expectedValue = Limits(
      choiceControllers = 123,
      choiceObservers = 234,
      contractObservers = 345,
      contractSignatories = 456,
      transactionInputContracts = 567,
    )
    val source = ConfigSource.fromConfig(value).cursor().value
    interpretationLimitsConvert.from(source).value shouldBe expectedValue
    interpretationLimitsConvert.to(expectedValue) shouldBe value.root()
  }

  behavior of "ContractKeyUniquenessMode"

  it should "read/write against predefined values" in {
    def compare(mode: ContractKeyUniquenessMode, expectedString: String): Assertion = {
      contractKeyUniquenessModeConvert.to(mode) shouldBe fromAnyRef(expectedString)
      contractKeyUniquenessModeConvert.from(fromAnyRef(expectedString)).value shouldBe mode
    }
    compare(ContractKeyUniquenessMode.Off, "off")
    compare(ContractKeyUniquenessMode.Strict, "strict")
  }

  behavior of "EngineConfig"

  it should "support current defaults" in {
    val value =
      """
        |allowed-language-versions = early-access
        |contract-key-uniqueness = strict
        |forbid-v-0-contract-id = true
        |limits {
        |  choice-controllers = 2147483647
        |  choice-observers = 2147483647
        |  contract-observers = 2147483647
        |  contract-signatories = 2147483647
        |  transaction-input-contracts = 2147483647
        |}
        |package-validation = true
        |require-suffixed-global-contract-id = false
        |stack-trace-mode = false
        |""".stripMargin

    convert(engineConvert, value).value shouldBe Config.DefaultEngineConfig
  }

  behavior of "MetricsReporter"

  it should "read/write against predefined values" in {
    def compare(
        reporter: MetricsReporter,
        expectedString: String,
    ): Assertion = {
      metricReporterWriter.to(reporter) shouldBe fromAnyRef(expectedString)
      metricReporterReader.from(fromAnyRef(expectedString)).value shouldBe reporter
    }
    compare(
      MetricsReporter.Prometheus(new InetSocketAddress("localhost", 1234)),
      "prometheus://localhost:1234",
    )
    compare(
      MetricsReporter.Graphite(new InetSocketAddress("localhost", 1234)),
      "graphite://localhost:1234/",
    )
    compare(
      MetricsReporter.Graphite(new InetSocketAddress("localhost", 1234), Some("test")),
      "graphite://localhost:1234/test",
    )
    val path = Path.of("test").toAbsolutePath
    compare(
      MetricsReporter.Csv(path),
      "csv://" + path.toString,
    )
    compare(MetricsReporter.Console, "console")
  }

  behavior of "MetricsConfig"

  it should "support current defaults" in {
    val value = """
     |    enabled = false
     |    reporter = console
     |    registry-type = jvm-shared
     |    reporting-interval = "10s"
     |""".stripMargin
    convert(metricsConvert, value).value shouldBe MetricsConfig()
  }

  behavior of "SecretsUrl"

  it should "read/write against predefined values" in {
    val secretUrl = "https://www.daml.com/secrets.json"
    secretsUrlReader.from(fromAnyRef(secretUrl)).value shouldBe SecretsUrl.fromString(secretUrl)
    secretsUrlWriter.to(SecretsUrl.fromString(secretUrl)) shouldBe fromAnyRef("<REDACTED>")
  }

  behavior of "InitialLedgerConfiguration"

  it should "support current defaults" in {
    val value = """
    |  enabled = true
    |  avg-transaction-latency = 0 days
    |  delay-before-submitting = 0 days
    |  max-deduplication-duration = 30 minutes
    |  max-skew = 30 seconds
    |  min-skew = 30 seconds
    |  """.stripMargin
    convert(initialLedgerConfigurationConvert, value).value shouldBe Some(
      InitialLedgerConfiguration()
    )
  }

  it should "read/write against predefined values" in {
    val value =
      """
          |enabled = true
          |avg-transaction-latency = 1 days
          |delay-before-submitting = 2 days
          |max-deduplication-duration = 3 minutes
          |max-skew = 4 seconds
          |min-skew = 5 seconds
          |""".stripMargin
    val expectedValue = InitialLedgerConfiguration(
      maxDeduplicationDuration = Duration.ofMinutes(3),
      avgTransactionLatency = Duration.ofDays(1),
      minSkew = Duration.ofSeconds(5),
      maxSkew = Duration.ofSeconds(4),
      delayBeforeSubmitting = Duration.ofDays(2),
    )
    convert(initialLedgerConfigurationConvert, value).value shouldBe Some(expectedValue)
  }

  behavior of "Seeding"

  it should "read/write against predefined values" in {
    seedingWriter.to(Seeding.Static) shouldBe fromAnyRef("testing-static")
    seedingWriter.to(Seeding.Weak) shouldBe fromAnyRef("testing-weak")
    seedingWriter.to(Seeding.Strong) shouldBe fromAnyRef("strong")
    seedingReader.from(fromAnyRef("testing-static")).value shouldBe Seeding.Static
    seedingReader.from(fromAnyRef("testing-weak")).value shouldBe Seeding.Weak
    seedingReader.from(fromAnyRef("strong")).value shouldBe Seeding.Strong
  }

  behavior of "userManagementConfig"

  it should "support current defaults" in {
    val value = """  
    |  cache-expiry-after-write-in-seconds = 5
    |  enabled = false
    |  max-cache-size = 100
    |  max-users-page-size = 1000""".stripMargin
    convert(userManagementConfigConvert, value).value shouldBe UserManagementConfig()
  }

  it should "read/write against predefined values" in {
    val value = """  
    |  cache-expiry-after-write-in-seconds = 1
    |  enabled = true
    |  max-cache-size = 99
    |  max-users-page-size = 999""".stripMargin

    convert(userManagementConfigConvert, value).value shouldBe UserManagementConfig(
      enabled = true,
      cacheExpiryAfterWriteInSeconds = 1,
      maxCacheSize = 99,
      maxUsersPageSize = 999,
    )
  }

  behavior of "AuthServiceConfig"

  it should "be isomorphic and support redaction" in forAll(ArbitraryConfig.authServiceConfig) {
    generatedValue =>
      val configValue = authServiceConfigConvert.to(generatedValue)
      val redacted = generatedValue match {
        case AuthServiceConfig.UnsafeJwtHmac256(_) =>
          AuthServiceConfig.UnsafeJwtHmac256("<REDACTED>")
        case _ => generatedValue
      }
      authServiceConfigConvert
        .from(configValue)
        .value shouldBe redacted
  }

  it should "read/write against predefined values" in {
    def compare(configString: String, expectedValue: AuthServiceConfig) = {
      val source =
        ConfigSource.fromConfig(ConfigFactory.parseString(configString)).cursor().value
      authServiceConfigConvert.from(source).value shouldBe expectedValue
    }

    compare("type = wildcard", AuthServiceConfig.Wildcard)
    compare(
      "type = unsafe-jwt-hmac-256\nsecret=mysecret",
      AuthServiceConfig.UnsafeJwtHmac256("mysecret"),
    )
    compare(
      "type = unsafe-jwt-hmac-256\nsecret=mysecret2",
      AuthServiceConfig.UnsafeJwtHmac256("mysecret2"),
    )
    compare(
      "type = jwt-rs-256\ncertificate=certfile",
      AuthServiceConfig.JwtRs256("certfile"),
    )
    compare(
      "type = jwt-es-256\ncertificate=certfile3",
      AuthServiceConfig.JwtEs256("certfile3"),
    )
    compare(
      "type = jwt-es-512\ncertificate=certfile4",
      AuthServiceConfig.JwtEs512("certfile4"),
    )
    compare(
      """
        |type = jwt-rs-256-jwks
        |url="https://daml.com/jwks.json"
        |""".stripMargin,
      AuthServiceConfig.JwtRs256Jwks("https://daml.com/jwks.json"),
    )
  }

  behavior of "PartyConfiguration"

  it should "read/write against predefined values" in {
    val value =
      ConfigFactory.parseString("implicit-party-allocation=false")
    val source = ConfigSource.fromConfig(value).cursor().value
    partyConfigurationConvert.from(source).value shouldBe PartyConfiguration()
  }

  behavior of "CommandConfiguration"

  it should "read/write against predefined values" in {
    val value =
      """ 
     |  input-buffer-size = 512
     |  max-commands-in-flight = 256
     |  tracker-retention-period = "300 seconds"""".stripMargin
    convert(commandConfigurationConvert, value).value shouldBe CommandConfiguration()
  }

  behavior of "TimeProviderType"

  it should "read/write against predefined values" in {
    timeProviderTypeConvert.to(TimeProviderType.Static) shouldBe fromAnyRef("static")
    timeProviderTypeConvert.to(TimeProviderType.WallClock) shouldBe fromAnyRef("wall-clock")
    timeProviderTypeConvert.from(fromAnyRef("static")).value shouldBe TimeProviderType.Static
    timeProviderTypeConvert.from(fromAnyRef("wall-clock")).value shouldBe TimeProviderType.WallClock
  }

  behavior of "SynchronousCommitValue"

  it should "read/write against predefined values" in {
    val conv = dbConfigSynchronousCommitValueConvert
    def compare(value: SynchronousCommitValue, str: String): Assertion = {
      conv.to(value) shouldBe fromAnyRef(str)
      conv.from(fromAnyRef(str)).value shouldBe value
    }
    compare(SynchronousCommitValue.On, "on")
    compare(SynchronousCommitValue.Off, "off")
    compare(SynchronousCommitValue.RemoteWrite, "remote-write")
    compare(SynchronousCommitValue.RemoteApply, "remote-apply")
    compare(SynchronousCommitValue.Local, "local")
  }

  behavior of "RateLimitingConfig"

  it should "support current defaults" in {
    val value = """ 
    |  enabled = true
    |  max-api-services-index-db-queue-size = 1000
    |  max-api-services-queue-size = 10000
    |  max-used-heap-space-percentage = 85
    |  min-free-heap-space-bytes = 314572800""".stripMargin
    convert(rateLimitingConfigConvert, value).value shouldBe Some(RateLimitingConfig())
  }

  behavior of "ApiServerConfig"

  it should "support current defaults" in {
    val value = """       
                                  |api-stream-shutdown-timeout = "5s"
                                  |command {
                                  |  input-buffer-size = 512
                                  |  max-commands-in-flight = 256
                                  |  tracker-retention-period = "300 seconds"
                                  |}
                                  |initial-ledger-configuration {
                                  |  enabled = true
                                  |  avg-transaction-latency = 0 days
                                  |  delay-before-submitting = 0 days
                                  |  max-deduplication-duration = 30 minutes
                                  |  max-skew = 30 seconds
                                  |  min-skew = 30 seconds
                                  |}
                                  |configuration-load-timeout = "10s"
                                  |management-service-timeout = "2m"
                                  |max-inbound-message-size = 67108864
                                  |party {
                                  |  implicit-party-allocation = false
                                  |}
                                  |port = 6865
                                  |rate-limit {
                                  |  enabled = true
                                  |  max-api-services-index-db-queue-size = 1000
                                  |  max-api-services-queue-size = 10000
                                  |  max-used-heap-space-percentage = 85
                                  |  min-free-heap-space-bytes = 314572800
                                  |}
                                  |seeding = strong
                                  |time-provider-type = wall-clock
                                  |user-management {
                                  |  cache-expiry-after-write-in-seconds = 5
                                  |  enabled = false
                                  |  max-cache-size = 100
                                  |  max-users-page-size = 1000
                                  |}""".stripMargin
    convert(apiServerConfigConvert, value).value shouldBe ApiServerConfig()
  }

  behavior of "HaConfig"

  it should "support current defaults" in {
    val value = """
    |  indexer-lock-id = 105305792
    |  indexer-worker-lock-id = 105305793
    |  main-lock-acquire-retry-millis = 500
    |  main-lock-checker-period-millis = 1000
    |  worker-lock-acquire-max-retry = 1000
    |  worker-lock-acquire-retry-millis = 500
    |  """.stripMargin
    convert(haConfigConvert, value).value shouldBe HaConfig()
  }

  behavior of "IndexerConfig"

  it should "support current defaults" in {
    val value = """
    |  batching-parallelism = 4
    |  enable-compression = false
    |  high-availability {
    |    indexer-lock-id = 105305792
    |    indexer-worker-lock-id = 105305793
    |    main-lock-acquire-retry-millis = 500
    |    main-lock-checker-period-millis = 1000
    |    worker-lock-acquire-max-retry = 1000
    |    worker-lock-acquire-retry-millis = 500
    |  }
    |  ingestion-parallelism = 16
    |  input-mapping-parallelism = 16
    |  max-input-buffer-size = 50
    |  restart-delay = "10s"
    |  startup-mode {
    |    allow-existing-schema = false
    |    type = migrate-and-start
    |  }
    |  submission-batch-size = 50""".stripMargin
    convert(indexerConfigConvert, value).value shouldBe IndexerConfig()
  }

  behavior of "IndexServiceConfig"

  it should "support current defaults" in {
    val value = """ 
    |  acs-contract-fetching-parallelism = 2
    |  acs-global-parallelism = 10
    |  acs-id-fetching-parallelism = 2
    |  acs-id-page-buffer-size = 1
    |  acs-id-page-size = 20000
    |  acs-id-page-working-memory-bytes = 104857600
    |  api-stream-shutdown-timeout = "5s"
    |  buffered-streams-page-size = 100
    |  enable-in-memory-fan-out-for-ledger-api = false
    |  events-page-size = 1000
    |  events-processing-parallelism = 8
    |  max-contract-key-state-cache-size = 100000
    |  max-contract-state-cache-size = 100000
    |  max-transactions-in-memory-fan-out-buffer-size = 10000""".stripMargin
    convert(indexServiceConfigConvert, value).value shouldBe IndexServiceConfig()
  }

  behavior of "ParticipantDataSourceConfig"

  it should "read/write against predefined values" in {
    val secretUrl = "https://www.daml.com/secrets.json"
    participantDataSourceConfigReader
      .from(fromAnyRef(secretUrl))
      .value shouldBe ParticipantDataSourceConfig(secretUrl)
    participantDataSourceConfigWriter.to(
      ParticipantDataSourceConfig(secretUrl)
    ) shouldBe fromAnyRef("<REDACTED>")
  }

  behavior of "optReaderEnabled/optWriterEnabled"
  case class Cfg(i: Int)
  case class Cfg2(enabled: Boolean, i: Int)
  import pureconfig.generic.semiauto._
  val testConvert: ConfigConvert[Cfg] = deriveConvert[Cfg]
  val testConvert2: ConfigConvert[Cfg2] = deriveConvert[Cfg2]

  it should "read enabled flag" in {
    val reader: ConfigReader[Option[Cfg]] = optReaderEnabled[Cfg](testConvert)
    convert(reader, "enabled = true\ni = 1").value shouldBe Some(Cfg(1))
    convert(reader, "enabled = true\ni = 10").value shouldBe Some(Cfg(10))
    convert(reader, "enabled = false\ni = 1").value shouldBe None
    convert(reader, "enabled = false").value shouldBe None
  }

  it should "write enabled flag" in {
    val writer: ConfigWriter[Option[Cfg]] = optWriterEnabled[Cfg](testConvert)
    writer.to(Some(Cfg(1))) shouldBe ConfigFactory.parseString("enabled = true\ni = 1").root()
    writer.to(Some(Cfg(10))) shouldBe ConfigFactory.parseString("enabled = true\ni = 10").root()
    writer.to(None) shouldBe ConfigFactory.parseString("enabled = false").root()
  }

  it should "throw if configuration is ambiguous" in {
    val writer: ConfigWriter[Option[Cfg2]] = optWriterEnabled[Cfg2](testConvert2)
    an[IllegalArgumentException] should be thrownBy writer.to(Some(Cfg2(enabled = false, 1)))
  }

}
