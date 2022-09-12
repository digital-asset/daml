// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.ContractKeyUniquenessMode
import com.daml.lf.{VersionRange, interpretation, language}
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.{ApiServerConfig, AuthServiceConfig}
import com.daml.platform.config.{MetricsConfig, ParticipantConfig}
import com.daml.platform.configuration.{
  CommandConfiguration,
  IndexServiceConfig,
  InitialLedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, PackageMetadataViewConfig}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port
import com.typesafe.config.{ConfigObject, ConfigValueFactory}
import io.netty.handler.ssl.ClientAuth
import pureconfig.configurable.{genericMapReader, genericMapWriter}
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto._
import pureconfig.{ConfigConvert, ConfigCursor, ConfigReader, ConfigWriter, ConvertHelpers}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Try

class PureConfigReaderWriter(secure: Boolean = true) {
  val Secret = "<REDACTED>"

  /** Reads configuration object of `T` and `enabled` flag to find out if this object has values.
    */
  def optReaderEnabled[T](reader: ConfigReader[T]): ConfigReader[Option[T]] =
    (cursor: ConfigCursor) =>
      for {
        objCur <- cursor.asObjectCursor
        enabledCur <- objCur.atKey("enabled")
        enabled <- enabledCur.asBoolean
        value <-
          if (enabled) {
            reader.from(cursor).map(x => Some(x))
          } else {
            Right(None)
          }
      } yield value

  /** Writes object of `T` and adds `enabled` flag for configuration which contains value.
    */
  def optWriterEnabled[T](writer: ConfigWriter[T]): ConfigWriter[Option[T]] = {
    import scala.jdk.CollectionConverters._
    def toConfigValue(enabled: Boolean) =
      ConfigValueFactory.fromMap(Map("enabled" -> enabled).asJava)
    (optValue: Option[T]) =>
      optValue match {
        case Some(value) =>
          writer.to(value) match {
            // if serialised object of `T` is `ConfigObject` and
            // has `enabled` inside, it cannot be supported by this writer
            case configObject: ConfigObject if configObject.toConfig.hasPath("enabled") =>
              throw new IllegalArgumentException(
                "Ambiguous configuration, object contains `enabled` flag"
              )
            case _ =>
              writer.to(value).withFallback(toConfigValue(enabled = true))
          }
        case None => toConfigValue(enabled = false)
      }
  }

  def optConvertEnabled[T](
      reader: ConfigReader[T],
      writer: ConfigWriter[T],
  ): ConfigConvert[Option[T]] =
    ConfigConvert.apply(optReaderEnabled(reader), optWriterEnabled(writer))

  def optConvertEnabled[T](convert: ConfigConvert[T]): ConfigConvert[Option[T]] =
    optConvertEnabled(convert, convert)

  implicit val javaDurationWriter: ConfigWriter[java.time.Duration] =
    ConfigWriter.stringConfigWriter.contramap[java.time.Duration] { duration =>
      duration.toScala.toString()
    }

  implicit val javaDurationReader: ConfigReader[java.time.Duration] =
    ConfigReader.fromString[java.time.Duration] { str =>
      Some(Duration.apply(str))
        .collect { case d: FiniteDuration => d }
        .map(_.toJava)
        .toRight(CannotConvert(str, Duration.getClass.getName, s"Could not convert $str"))
    }

  implicit val versionRangeReader: ConfigReader[VersionRange[language.LanguageVersion]] =
    ConfigReader.fromString[VersionRange[LanguageVersion]] {
      case "daml-lf-dev-mode-unsafe" => Right(LanguageVersion.DevVersions)
      case "early-access" => Right(LanguageVersion.EarlyAccessVersions)
      case "stable" => Right(LanguageVersion.StableVersions)
      case "legacy" => Right(LanguageVersion.LegacyVersions)
      case value if value.split("-").length == 2 =>
        val Array(min, max) = value.split("-")
        val convertedValue: Either[String, VersionRange[LanguageVersion]] = for {
          min <- language.LanguageVersion.fromString(min)
          max <- language.LanguageVersion.fromString(max)
        } yield {
          VersionRange[language.LanguageVersion](min, max)
        }
        convertedValue.left.map { error =>
          CannotConvert(value, VersionRange.getClass.getName, s"$value is not recognized. " + error)
        }
      case otherwise =>
        Left(
          CannotConvert(otherwise, VersionRange.getClass.getName, s"$otherwise is not recognized. ")
        )
    }

  implicit val versionRangeWriter: ConfigWriter[VersionRange[language.LanguageVersion]] =
    ConfigWriter.toString {
      case LanguageVersion.DevVersions => "daml-lf-dev-mode-unsafe"
      case LanguageVersion.EarlyAccessVersions => "early-access"
      case LanguageVersion.StableVersions => "stable"
      case LanguageVersion.LegacyVersions => "legacy"
      case range => s"${range.min.pretty}-${range.max.pretty}"
    }

  implicit val interpretationLimitsConvert: ConfigConvert[interpretation.Limits] =
    deriveConvert[interpretation.Limits]

  implicit val contractKeyUniquenessModeConvert: ConfigConvert[ContractKeyUniquenessMode] =
    deriveEnumerationConvert[ContractKeyUniquenessMode]

  implicit val engineConvert: ConfigConvert[EngineConfig] = deriveConvert[EngineConfig]

  implicit val metricReporterReader: ConfigReader[MetricsReporter] = {
    ConfigReader.fromString[MetricsReporter](ConvertHelpers.catchReadError { s =>
      MetricsReporter.parseMetricsReporter(s)
    })
  }
  implicit val metricReporterWriter: ConfigWriter[MetricsReporter] =
    ConfigWriter.toString {
      case MetricsReporter.Console => "console"
      case MetricsReporter.Csv(directory) => s"csv://${directory.toAbsolutePath.toString}"
      case MetricsReporter.Graphite(address, prefix) =>
        s"graphite://${address.getHostName}:${address.getPort}/${prefix.getOrElse("")}"
      case MetricsReporter.Prometheus(address) =>
        s"prometheus://${address.getHostName}:${address.getPort}"
    }

  implicit val metricsRegistryTypeConvert: ConfigConvert[MetricsConfig.MetricRegistryType] =
    deriveEnumerationConvert[MetricsConfig.MetricRegistryType]

  implicit val metricsConvert: ConfigConvert[MetricsConfig] = deriveConvert[MetricsConfig]

  implicit val secretsUrlReader: ConfigReader[SecretsUrl] =
    ConfigReader.fromString[SecretsUrl] { url =>
      Right(SecretsUrl.fromString(url))
    }

  implicit val secretsUrlWriter: ConfigWriter[SecretsUrl] =
    ConfigWriter.toString {
      case SecretsUrl.FromUrl(url) if !secure => url.toString
      case _ => Secret
    }

  implicit val clientAuthReader: ConfigReader[ClientAuth] =
    ConfigReader.fromStringTry[ClientAuth](value => Try(ClientAuth.valueOf(value.toUpperCase)))
  implicit val clientAuthWriter: ConfigWriter[ClientAuth] =
    ConfigWriter.toString(_.name().toLowerCase)

  implicit val tlsVersionReader: ConfigReader[TlsVersion] =
    ConfigReader.fromString[TlsVersion] { tlsVersion =>
      TlsVersion.allVersions
        .find(_.version == tlsVersion)
        .toRight(
          CannotConvert(tlsVersion, TlsVersion.getClass.getName, s"$tlsVersion is not recognized.")
        )
    }

  implicit val tlsVersionWriter: ConfigWriter[TlsVersion] =
    ConfigWriter.toString(tlsVersion => tlsVersion.version)

  implicit val tlsConfigurationConvert: ConfigConvert[TlsConfiguration] =
    deriveConvert[TlsConfiguration]

  implicit val portReader: ConfigReader[Port] = ConfigReader.intConfigReader.map(Port.apply)
  implicit val portWriter: ConfigWriter[Port] = ConfigWriter.intConfigWriter.contramap[Port] {
    port: Port => port.value
  }

  implicit val initialLedgerConfigurationConvert
      : ConfigConvert[Option[InitialLedgerConfiguration]] =
    optConvertEnabled(deriveConvert[InitialLedgerConfiguration])

  implicit val seedingReader: ConfigReader[Seeding] =
    // Not using deriveEnumerationReader[Seeding] as we prefer "testing-static" over static (that appears
    // in Seeding.name, but not in the case object name).
    ConfigReader.fromString[Seeding] {
      case Seeding.Strong.name => Right(Seeding.Strong)
      case Seeding.Weak.name => Right(Seeding.Weak)
      case Seeding.Static.name => Right(Seeding.Static)
      case unknownSeeding =>
        Left(
          CannotConvert(
            unknownSeeding,
            Seeding.getClass.getName,
            s"Seeding is neither ${Seeding.Strong.name}, ${Seeding.Weak.name}, nor ${Seeding.Static.name}: ${unknownSeeding}",
          )
        )
    }

  implicit val seedingWriter: ConfigWriter[Seeding] = ConfigWriter.toString(_.name)

  implicit val userManagementConfigConvert: ConfigConvert[UserManagementConfig] =
    deriveConvert[UserManagementConfig]

  implicit val jwtTimestampLeewayConfigConvert: ConfigConvert[Option[JwtTimestampLeeway]] =
    optConvertEnabled(deriveConvert[JwtTimestampLeeway])

  implicit val authServiceConfigUnsafeJwtHmac256Reader
      : ConfigReader[AuthServiceConfig.UnsafeJwtHmac256] =
    deriveReader[AuthServiceConfig.UnsafeJwtHmac256]
  implicit val authServiceConfigUnsafeJwtHmac256Writer
      : ConfigWriter[AuthServiceConfig.UnsafeJwtHmac256] =
    deriveWriter[AuthServiceConfig.UnsafeJwtHmac256].contramap[AuthServiceConfig.UnsafeJwtHmac256] {
      case x if secure => x.copy(secret = Secret)
      case x => x
    }
  implicit val authServiceConfigJwtEs256CrtConvert: ConfigConvert[AuthServiceConfig.JwtEs256] =
    deriveConvert[AuthServiceConfig.JwtEs256]
  implicit val authServiceConfigJwtEs512CrtConvert: ConfigConvert[AuthServiceConfig.JwtEs512] =
    deriveConvert[AuthServiceConfig.JwtEs512]
  implicit val authServiceConfigJwtRs256CrtConvert: ConfigConvert[AuthServiceConfig.JwtRs256] =
    deriveConvert[AuthServiceConfig.JwtRs256]
  implicit val authServiceConfigJwtRs256JwksConvert: ConfigConvert[AuthServiceConfig.JwtRs256Jwks] =
    deriveConvert[AuthServiceConfig.JwtRs256Jwks]
  implicit val authServiceConfigWildcardConvert: ConfigConvert[AuthServiceConfig.Wildcard.type] =
    deriveConvert[AuthServiceConfig.Wildcard.type]
  implicit val authServiceConfigConvert: ConfigConvert[AuthServiceConfig] =
    deriveConvert[AuthServiceConfig]

  implicit val partyConfigurationConvert: ConfigConvert[PartyConfiguration] =
    deriveConvert[PartyConfiguration]

  implicit val commandConfigurationConvert: ConfigConvert[CommandConfiguration] =
    deriveConvert[CommandConfiguration]

  implicit val timeProviderTypeConvert: ConfigConvert[TimeProviderType] =
    deriveEnumerationConvert[TimeProviderType]

  implicit val dbConfigSynchronousCommitValueConvert: ConfigConvert[SynchronousCommitValue] =
    deriveEnumerationConvert[SynchronousCommitValue]

  implicit val dbConfigConnectionPoolConfigConvert: ConfigConvert[ConnectionPoolConfig] =
    deriveConvert[ConnectionPoolConfig]

  implicit val dbConfigPostgresDataSourceConfigConvert: ConfigConvert[PostgresDataSourceConfig] =
    deriveConvert[PostgresDataSourceConfig]

  implicit val dataSourcePropertiesConvert: ConfigConvert[DataSourceProperties] =
    deriveConvert[DataSourceProperties]

  implicit val rateLimitingConfigConvert: ConfigConvert[Option[RateLimitingConfig]] =
    optConvertEnabled(deriveConvert[RateLimitingConfig])

  implicit val apiServerConfigConvert: ConfigConvert[ApiServerConfig] =
    deriveConvert[ApiServerConfig]

  implicit val validateAndStartConvert: ConfigConvert[IndexerStartupMode.ValidateAndStart.type] =
    deriveConvert[IndexerStartupMode.ValidateAndStart.type]

  implicit val MigrateOnEmptySchemaAndStartReader
      : ConfigConvert[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type] =
    deriveConvert[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type]

  implicit val migrateAndStartConvert: ConfigConvert[IndexerStartupMode.MigrateAndStart] =
    deriveConvert[IndexerStartupMode.MigrateAndStart]

  implicit val validateAndWaitOnlyConvert: ConfigConvert[IndexerStartupMode.ValidateAndWaitOnly] =
    deriveConvert[IndexerStartupMode.ValidateAndWaitOnly]

  implicit val indexerStartupModeConvert: ConfigConvert[IndexerStartupMode] =
    deriveConvert[IndexerStartupMode]

  implicit val haConfigConvert: ConfigConvert[HaConfig] = deriveConvert[HaConfig]

  private def createParticipantId(participantId: String) =
    Ref.ParticipantId
      .fromString(participantId)
      .left
      .map(err => CannotConvert(participantId, Ref.ParticipantId.getClass.getName, err))

  implicit val participantIdReader: ConfigReader[Ref.ParticipantId] = ConfigReader
    .fromString[Ref.ParticipantId](createParticipantId)

  implicit val participantIdWriter: ConfigWriter[Ref.ParticipantId] =
    ConfigWriter.toString[Ref.ParticipantId](_.toString)

  implicit val packageMetadataViewConfigConvert: ConfigConvert[PackageMetadataViewConfig] =
    deriveConvert[PackageMetadataViewConfig]

  implicit val indexerConfigConvert: ConfigConvert[IndexerConfig] = deriveConvert[IndexerConfig]

  implicit val indexServiceConfigConvert: ConfigConvert[IndexServiceConfig] =
    deriveConvert[IndexServiceConfig]

  implicit val participantConfigConvert: ConfigConvert[ParticipantConfig] =
    deriveConvert[ParticipantConfig]

  implicit val participantDataSourceConfigReader: ConfigReader[ParticipantDataSourceConfig] =
    ConfigReader.fromString[ParticipantDataSourceConfig] { url =>
      Right(ParticipantDataSourceConfig(url))
    }

  implicit val participantDataSourceConfigWriter: ConfigWriter[ParticipantDataSourceConfig] =
    ConfigWriter.toString {
      case _ if secure => Secret
      case dataSourceConfig => dataSourceConfig.jdbcUrl
    }

  implicit val participantDataSourceConfigMapReader
      : ConfigReader[Map[Ref.ParticipantId, ParticipantDataSourceConfig]] =
    genericMapReader[Ref.ParticipantId, ParticipantDataSourceConfig]((s: String) =>
      createParticipantId(s)
    )
  implicit val participantDataSourceConfigMapWriter
      : ConfigWriter[Map[Ref.ParticipantId, ParticipantDataSourceConfig]] =
    genericMapWriter[Ref.ParticipantId, ParticipantDataSourceConfig](_.toString)

  implicit val participantConfigMapReader: ConfigReader[Map[Ref.ParticipantId, ParticipantConfig]] =
    genericMapReader[Ref.ParticipantId, ParticipantConfig]((s: String) => createParticipantId(s))
  implicit val participantConfigMapWriter: ConfigWriter[Map[Ref.ParticipantId, ParticipantConfig]] =
    genericMapWriter[Ref.ParticipantId, ParticipantConfig](_.toString)

  implicit val configConvert: ConfigConvert[Config] = deriveConvert[Config]
}

object PureConfigReaderWriter {
  implicit val Secure = new PureConfigReaderWriter(secure = true)
}
