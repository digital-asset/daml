// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.ledger.api.tls.{TlsConfiguration, TlsVersion}
import com.digitalasset.canton.ledger.runner.common.OptConfigValue.{
  optReaderEnabled,
  optWriterEnabled,
}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  IndexServiceConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.{IndexerConfig, PackageMetadataViewConfig}
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigConvert, ConfigReader, ConfigSource, ConfigWriter}

import java.net.InetSocketAddress
import java.nio.file.Path
import java.time.Duration
import scala.annotation.nowarn
import scala.reflect.{ClassTag, classTag}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
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
      secure: Boolean,
      generator: Gen[T],
      name: Option[String] = None,
  ): Unit = {
    val secureText = secure match {
      case true => "secure "
      case false => ""
    }
    secureText + name.getOrElse(classTag[T].toString) should "be isomorphic" in forAll(generator) {
      generatedValue =>
        val writer = implicitly[ConfigWriter[T]]
        val reader = implicitly[ConfigReader[T]]
        reader.from(writer.to(generatedValue)).value shouldBe generatedValue
    }
  }

  def testReaderWriterIsomorphism(secure: Boolean): Unit = {
    val readerWriter = new PureConfigReaderWriter(secure)
    import readerWriter.*
    testReaderWriterIsomorphism(secure, ArbitraryConfig.duration)
    testReaderWriterIsomorphism(secure, Gen.oneOf(TlsVersion.allVersions))
    testReaderWriterIsomorphism(secure, ArbitraryConfig.tlsConfiguration)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.port)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.clientAuth)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.userManagementServiceConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.identityProviderManagementConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.connectionPoolConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.postgresDataSourceConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.dataSourceProperties)
    testReaderWriterIsomorphism(
      secure,
      ArbitraryConfig.rateLimitingConfig,
      Some("RateLimitingConfig"),
    )
    testReaderWriterIsomorphism(secure, ArbitraryConfig.indexerConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.packageMetadataViewConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.commandServiceConfig)
    testReaderWriterIsomorphism(secure, ArbitraryConfig.indexServiceConfig)
  }

  testReaderWriterIsomorphism(secure = true)
  testReaderWriterIsomorphism(secure = false)

  import PureConfigReaderWriter.Secure.*

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

  behavior of "JwtTimestampLeeway"

  val validJwtTimestampLeewayValue =
    """
      |  enabled = true
      |  default = 1
      |""".stripMargin

  it should "read/write against predefined values" in {
    def compare(configString: String, expectedValue: Option[JwtTimestampLeeway]) = {
      convert(jwtTimestampLeewayConfigConvert, configString).value shouldBe expectedValue

    }
    compare(
      """
        |  enabled = true
        |  default = 1
        |""".stripMargin,
      Some(JwtTimestampLeeway(Some(1), None, None, None)),
    )
    compare(
      """
        |  enabled = true
        |  expires-at = 2
        |""".stripMargin,
      Some(JwtTimestampLeeway(None, Some(2), None, None)),
    )
    compare(
      """
        |  enabled = true
        |  issued-at = 3
        |""".stripMargin,
      Some(JwtTimestampLeeway(None, None, Some(3), None)),
    )
    compare(
      """
        |  enabled = true
        |  not-before = 4
        |""".stripMargin,
      Some(JwtTimestampLeeway(None, None, None, Some(4))),
    )
    compare(
      """
        |  enabled = true
        |  default = 1
        |  expires-at = 2
        |  issued-at = 3
        |  not-before = 4
        |""".stripMargin,
      Some(JwtTimestampLeeway(Some(1), Some(2), Some(3), Some(4))),
    )
    compare(
      """
        |  enabled = false
        |  default = 1
        |  expires-at = 2
        |  issued-at = 3
        |  not-before = 4
        |""".stripMargin,
      None,
    )
  }

  it should "not support unknown keys" in {
    convert(
      jwtTimestampLeewayConfigConvert,
      "unknown-key=yes\n" + validJwtTimestampLeewayValue,
    ).left.value
      .prettyPrint(0) should include("Unknown key")
  }

  behavior of "TlsConfiguration"

  val validTlsConfigurationValue =
    """enabled=false
      |client-auth=require
      |enable-cert-revocation-checking=false""".stripMargin

  it should "read/write against predefined values" in {
    convert(
      tlsConfigurationConvert,
      validTlsConfigurationValue,
    ).value shouldBe TlsConfiguration(enabled = false)
  }

  it should "not support invalid unknown keys" in {
    convert(
      tlsConfigurationConvert,
      "unknown-key=yes\n" + validTlsConfigurationValue,
    ).left.value
      .prettyPrint(0) should include("Unknown key")
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

  behavior of "userManagementServiceConfig"

  val validUserManagementServiceConfigValue =
    """
      |  cache-expiry-after-write-in-seconds = 5
      |  enabled = true
      |  max-cache-size = 100
      |  max-users-page-size = 1000""".stripMargin

  it should "support current defaults" in {
    val value = validUserManagementServiceConfigValue
    convert(userManagementServiceConfigConvert, value).value shouldBe UserManagementServiceConfig()
  }

  it should "not support invalid keys" in {
    val value = "unknown-key=yes\n" + validUserManagementServiceConfigValue
    convert(userManagementServiceConfigConvert, value).left.value
      .prettyPrint(0) should include("Unknown key")
  }

  it should "read/write against predefined values" in {
    val value = """
    |  cache-expiry-after-write-in-seconds = 1
    |  enabled = true
    |  max-cache-size = 99
    |  max-users-page-size = 999""".stripMargin

    convert(userManagementServiceConfigConvert, value).value shouldBe UserManagementServiceConfig(
      enabled = true,
      cacheExpiryAfterWriteInSeconds = 1,
      maxCacheSize = 99,
      maxUsersPageSize = 999,
    )
  }

  behavior of "PostgresDataSourceConfig"

  val validPostgresDataSourceConfigValue =
    """
      |  tcp-keepalives-idle = 10
      |  tcp-keepalives-interval = 1
      |  tcp-keepalives-count = 5""".stripMargin

  it should "support current defaults" in {
    val value = validPostgresDataSourceConfigValue
    convert(
      dbConfigPostgresDataSourceConfigConvert,
      value,
    ).value shouldBe PostgresDataSourceConfig()
  }

  it should "not support invalid keys" in {
    val value = "unknown-key=yes\n" + validPostgresDataSourceConfigValue
    convert(dbConfigPostgresDataSourceConfigConvert, value).left.value
      .prettyPrint(0) should include("Unknown key")
  }

  it should "read/write against predefined values" in {
    val value =
      """
        |  synchronous-commit = on
        |  tcp-keepalives-idle = 9
        |  tcp-keepalives-interval = 99
        |  tcp-keepalives-count = 999""".stripMargin

    convert(dbConfigPostgresDataSourceConfigConvert, value).value shouldBe PostgresDataSourceConfig(
      synchronousCommit = Some(SynchronousCommitValue.On),
      tcpKeepalivesIdle = Some(9),
      tcpKeepalivesInterval = Some(99),
      tcpKeepalivesCount = Some(999),
    )
  }

  it should "read/write against some predefined values and some defaults" in {
    val value =
      """
        |  synchronous-commit = on
        |  tcp-keepalives-idle = 9""".stripMargin

    convert(dbConfigPostgresDataSourceConfigConvert, value).value shouldBe PostgresDataSourceConfig(
      synchronousCommit = Some(SynchronousCommitValue.On),
      tcpKeepalivesIdle = Some(9),
      tcpKeepalivesInterval = PostgresDataSourceConfig().tcpKeepalivesInterval,
      tcpKeepalivesCount = PostgresDataSourceConfig().tcpKeepalivesCount,
    )
  }

  behavior of "CommandServiceConfig"

  val validCommandConfigurationValue =
    """
      |  default-tracking-timeout = "300 seconds"
      |  max-commands-in-flight = 256""".stripMargin

  it should "read/write against predefined values" in {
    val value = validCommandConfigurationValue
    convert(commandConfigurationConvert, value).value shouldBe CommandServiceConfig()
  }

  it should "not support additional unknown keys" in {
    val value = "unknown-key=yes\n" + validCommandConfigurationValue
    convert(commandConfigurationConvert, value).left.value
      .prettyPrint(0) should include("Unknown key")
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

  val validRateLimitingConfig =
    """
      |  enabled = true
      |  max-api-services-index-db-queue-size = 1000
      |  max-api-services-queue-size = 10000
      |  max-used-heap-space-percentage = 85
      |  min-free-heap-space-bytes = 300000""".stripMargin

  it should "support current defaults" in {
    val value = validRateLimitingConfig
    val expected = RateLimitingConfig(
      maxApiServicesQueueSize = 10000,
      maxApiServicesIndexDbQueueSize = 1000,
      maxUsedHeapSpacePercentage = 85,
      minFreeHeapSpaceBytes = 300000,
      maxStreams = 1000,
    )
    convert(rateLimitingConfigConvert, value).value shouldBe Some(expected)
  }

  it should "not support unknown keys" in {
    val value = "unknown-key=yes\n" + validRateLimitingConfig
    convert(rateLimitingConfigConvert, value).left.value.prettyPrint(0) should include(
      "Unknown key"
    )
  }

  behavior of "HaConfig"

  val validHaConfigValue =
    """
      |  indexer-lock-id = 105305792
      |  indexer-worker-lock-id = 105305793
      |  main-lock-acquire-retry-timeout= 500 milliseconds
      |  main-lock-checker-period = 1000 milliseconds
      |  worker-lock-acquire-max-retries = 1000
      |  worker-lock-acquire-retry-timeout = 500 milliseconds
      |  main-lock-checker-jdbc-network-timeout = 10000 milliseconds
      |  """.stripMargin

  it should "support current defaults" in {
    val value = validHaConfigValue
    convert(haConfigConvert, value).value shouldBe HaConfig()
  }

  it should "not support unknown keys" in {
    val value = "unknown-key=yes\n" + validHaConfigValue
    convert(haConfigConvert, value).left.value.prettyPrint(0) should include("Unknown key")
  }

  behavior of "PackageMetadataViewConfig"

  val validPackageMetadataViewConfigValue =
    """
      |  init-load-parallelism = 16
      |  init-process-parallelism = 16
      |  init-takes-too-long-initial-delay = 1 minute
      |  init-takes-too-long-interval = 10 seconds
      |  """.stripMargin

  it should "support current defaults" in {
    val value = validPackageMetadataViewConfigValue
    convert(packageMetadataViewConfigConvert, value).value shouldBe PackageMetadataViewConfig()
  }

  it should "not support unknown keys" in {
    val value = "unknown-key=yes\n" + validPackageMetadataViewConfigValue
    convert(packageMetadataViewConfigConvert, value).left.value.prettyPrint(0) should include(
      "Unknown key"
    )
  }

  behavior of "IndexerConfig"

  val validIndexerConfigValue =
    """
      |  batching-parallelism = 4
      |  enable-compression = false
      |  ingestion-parallelism = 16
      |  input-mapping-parallelism = 16
      |  max-input-buffer-size = 50
      |  restart-delay = "10s"
      |  submission-batch-size = 50""".stripMargin

  it should "support current defaults" in {
    val value = validIndexerConfigValue
    convert(indexerConfigConvert, value).value shouldBe IndexerConfig()
  }

  it should "not support unknown keys" in {
    val value = "unknown-key=yes\n" + validIndexerConfigValue
    convert(indexerConfigConvert, value).left.value.prettyPrint(0) should include(
      "Unknown key"
    )
  }

  behavior of "IndexServiceConfig"

  val validIndexServiceConfigValue =
    """|
      |active-contracts-service-streams {
      |    contract-processing-parallelism=8
      |    max-ids-per-id-page=20000
      |    max-pages-per-id-pages-buffer=1
      |    max-parallel-id-create-queries=2
      |    max-parallel-payload-create-queries=2
      |    max-payloads-per-payloads-page=1000
      |    max-working-memory-in-bytes-for-id-pages=104857600
      |}
      |api-stream-shutdown-timeout="5s"
      |buffered-streams-page-size=100
      |completions-page-size=1000
      |buffered-events-processing-parallelism=8
      |global-max-event-id-queries=20
      |global-max-event-payload-queries=10
      |in-memory-state-updater-parallelism=2
      |max-contract-key-state-cache-size=10000
      |max-contract-state-cache-size=10000
      |max-transactions-in-memory-fan-out-buffer-size=1000
      |prepare-package-metadata-time-out-warning="5s"
      |transaction-flat-streams {
      |    max-ids-per-id-page=20000
      |    max-pages-per-id-pages-buffer=1
      |    max-parallel-id-consuming-queries=4
      |    max-parallel-id-create-queries=4
      |    max-parallel-payload-consuming-queries=2
      |    max-parallel-payload-create-queries=2
      |    max-parallel-payload-queries=2
      |    max-payloads-per-payloads-page=1000
      |    max-working-memory-in-bytes-for-id-pages=104857600
      |    transactions-processing-parallelism=8
      |}
      |transaction-tree-streams {
      |    max-ids-per-id-page=20000
      |    max-pages-per-id-pages-buffer=1
      |    max-parallel-id-consuming-queries=8
      |    max-parallel-id-create-queries=8
      |    max-parallel-id-non-consuming-queries=4
      |    max-parallel-payload-consuming-queries=2
      |    max-parallel-payload-create-queries=2
      |    max-parallel-payload-non-consuming-queries=2
      |    max-parallel-payload-queries=2
      |    max-payloads-per-payloads-page=1000
      |    max-working-memory-in-bytes-for-id-pages=104857600
      |    transactions-processing-parallelism=8
      |}""".stripMargin

  it should "support current defaults" in {
    val value = validIndexServiceConfigValue
    convert(indexServiceConfigConvert, value).value shouldBe IndexServiceConfig()
  }

  it should "not support unknown keys" in {
    val value = "unknown-key=yes\n" + validIndexServiceConfigValue
    convert(indexServiceConfigConvert, value).left.value.prettyPrint(0) should include(
      "Unknown key"
    )
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
    new PureConfigReaderWriter(false).participantDataSourceConfigWriter.to(
      ParticipantDataSourceConfig(secretUrl)
    ) shouldBe fromAnyRef(secretUrl)
  }

  behavior of "optReaderEnabled/optWriterEnabled"
  case class Cfg(i: Int)
  case class Cfg2(enabled: Boolean, i: Int)
  import pureconfig.generic.semiauto.*
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
