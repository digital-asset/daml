// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.caching.SizedCache
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.ContractKeyUniquenessMode
import com.daml.lf.{VersionRange, interpretation, language}
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.{ApiServerConfig, AuthServiceConfig}
import com.daml.platform.configuration.{
  CommandConfiguration,
  IndexConfiguration,
  InitialLedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import pureconfig.configurable.{genericMapReader, genericMapWriter}
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto._
import pureconfig.{ConfigReader, ConfigWriter, ConvertHelpers}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Try

object PureConfigReaderWriter {
  val Secret = "****"

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
      case value =>
        Left(CannotConvert(value, VersionRange.getClass.getName, s"$value is not recognized."))
    }

  implicit val versionRangeWriter: ConfigWriter[VersionRange[language.LanguageVersion]] =
    ConfigWriter.toString {
      case LanguageVersion.DevVersions => "daml-lf-dev-mode-unsafe"
      case LanguageVersion.EarlyAccessVersions => "early-access"
      case LanguageVersion.StableVersions => "stable"
      case LanguageVersion.LegacyVersions => "legacy"
      case other => sys.error(s"Could not find $other in the list of LanguageVersion")
    }

  implicit val interpretationLimitsReader: ConfigReader[interpretation.Limits] =
    deriveReader[interpretation.Limits]
  implicit val interpretationLimitsWriter: ConfigWriter[interpretation.Limits] =
    deriveWriter[interpretation.Limits]

  implicit val contractKeyUniquenessModeReader: ConfigReader[ContractKeyUniquenessMode] =
    deriveEnumerationReader[ContractKeyUniquenessMode]
  implicit val contractKeyUniquenessModeWriter: ConfigWriter[ContractKeyUniquenessMode] =
    deriveEnumerationWriter[ContractKeyUniquenessMode]

  implicit val engineReader: ConfigReader[EngineConfig] = deriveReader[EngineConfig]
  implicit val engineWriter: ConfigWriter[EngineConfig] = deriveWriter[EngineConfig]

  implicit val metricReporterReader: ConfigReader[MetricsReporter] = {
    ConfigReader.fromString[MetricsReporter](ConvertHelpers.catchReadError { s =>
      MetricsReporter.parseMetricsReporter(s.toLowerCase())
    })
  }
  implicit val metricReporterWriter: ConfigWriter[MetricsReporter] =
    ConfigWriter.toString {
      case MetricsReporter.Console => "console"
      case MetricsReporter.Csv(directory) => s"csv://${directory.toAbsolutePath.toString}"
      case MetricsReporter.Graphite(address, _) => s"graphite://${address.toString}"
      case MetricsReporter.Prometheus(address) => s"prometheus://${address.toString}"
    }

  implicit val metricsReader: ConfigReader[MetricsConfig] = deriveReader[MetricsConfig]
  implicit val metricsWriter: ConfigWriter[MetricsConfig] = deriveWriter[MetricsConfig]

  implicit val secretsUrlReader: ConfigReader[SecretsUrl] =
    ConfigReader.fromString[SecretsUrl] { url =>
      Right(SecretsUrl.fromString(url))
    }

  implicit val secretsUrlWriter: ConfigWriter[SecretsUrl] =
    ConfigWriter.toString(_ => Secret)

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

  implicit val tlsConfigurationReader: ConfigReader[TlsConfiguration] =
    deriveReader[TlsConfiguration]
  implicit val tlsConfigurationWriter: ConfigWriter[TlsConfiguration] =
    deriveWriter[TlsConfiguration]

  implicit val portReader: ConfigReader[Port] = ConfigReader.intConfigReader.map(Port.apply)
  implicit val portWriter: ConfigWriter[Port] = ConfigWriter.intConfigWriter.contramap[Port] {
    port: Port => port.value
  }

  implicit val initialLedgerConfigurationReader: ConfigReader[InitialLedgerConfiguration] =
    deriveReader[InitialLedgerConfiguration]
  implicit val initialLedgerConfigurationWriter: ConfigWriter[InitialLedgerConfiguration] =
    deriveWriter[InitialLedgerConfiguration]

  implicit val contractIdSeedingReader: ConfigReader[Seeding] =
    // Not using deriveEnumerationReader[Seeding] as we prefer "testing-static" over static (that appears
    // in Seeding.name, but not in the case object name). This makes it clear that static is not to
    // be used in production and avoids naming the configuration option contractIdSeedingOverrideOnlyForTesting or so.
    ConfigReader.fromString[Seeding] {
      case Seeding.Strong.name => Right(Seeding.Strong)
      case Seeding.Weak.name =>
        Right(
          Seeding.Weak
        ) // Pending upstream discussions, weak may turn out to be viable too for production
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

  implicit val contractIdSeedingWriter: ConfigWriter[Seeding] = ConfigWriter.toString(_.name)

  implicit val userManagementConfigReader: ConfigReader[UserManagementConfig] =
    deriveReader[UserManagementConfig]
  implicit val userManagementConfigWriter: ConfigWriter[UserManagementConfig] =
    deriveWriter[UserManagementConfig]

  implicit val authServiceConfigUnsafeJwtHmac256Reader
      : ConfigReader[AuthServiceConfig.UnsafeJwtHmac256] =
    deriveReader[AuthServiceConfig.UnsafeJwtHmac256]
  implicit val authServiceConfigJwtEs256CrtReader: ConfigReader[AuthServiceConfig.JwtEs256Crt] =
    deriveReader[AuthServiceConfig.JwtEs256Crt]
  implicit val authServiceConfigJwtEs512CrtReader: ConfigReader[AuthServiceConfig.JwtEs512Crt] =
    deriveReader[AuthServiceConfig.JwtEs512Crt]
  implicit val authServiceConfigJwtRs256CrtReader: ConfigReader[AuthServiceConfig.JwtRs256Crt] =
    deriveReader[AuthServiceConfig.JwtRs256Crt]
  implicit val authServiceConfigJwtRs256JwksReader: ConfigReader[AuthServiceConfig.JwtRs256Jwks] =
    deriveReader[AuthServiceConfig.JwtRs256Jwks]
  implicit val authServiceConfigWildcardReader: ConfigReader[AuthServiceConfig.Wildcard.type] =
    deriveReader[AuthServiceConfig.Wildcard.type]
  implicit val authServiceConfigReader: ConfigReader[AuthServiceConfig] =
    deriveReader[AuthServiceConfig]

  implicit val authServiceConfigJwtEs256CrtWriter: ConfigWriter[AuthServiceConfig.JwtEs256Crt] =
    deriveWriter[AuthServiceConfig.JwtEs256Crt]
  implicit val authServiceConfigJwtEs512CrtWriter: ConfigWriter[AuthServiceConfig.JwtEs512Crt] =
    deriveWriter[AuthServiceConfig.JwtEs512Crt]
  implicit val authServiceConfigJwtRs256CrtWriter: ConfigWriter[AuthServiceConfig.JwtRs256Crt] =
    deriveWriter[AuthServiceConfig.JwtRs256Crt]
  implicit val authServiceConfigJwtRs256JwksWriter: ConfigWriter[AuthServiceConfig.JwtRs256Jwks] =
    deriveWriter[AuthServiceConfig.JwtRs256Jwks]
  implicit val authServiceConfigUnsafeJwtHmac256Writer
      : ConfigWriter[AuthServiceConfig.UnsafeJwtHmac256] =
    deriveWriter[AuthServiceConfig.UnsafeJwtHmac256]
  implicit val authServiceConfigWildcardWriter: ConfigWriter[AuthServiceConfig.Wildcard.type] =
    deriveWriter[AuthServiceConfig.Wildcard.type]
  implicit val authServiceConfigWriter: ConfigWriter[AuthServiceConfig] =
    deriveWriter[AuthServiceConfig]

  implicit val partyConfigurationReader: ConfigReader[PartyConfiguration] =
    deriveReader[PartyConfiguration]
  implicit val partyConfigurationWriter: ConfigWriter[PartyConfiguration] =
    deriveWriter[PartyConfiguration]

  implicit val commandConfigurationReader: ConfigReader[CommandConfiguration] =
    deriveReader[CommandConfiguration]
  implicit val commandConfigurationWriter: ConfigWriter[CommandConfiguration] =
    deriveWriter[CommandConfiguration]

  implicit val timeProviderTypeReader: ConfigReader[TimeProviderType] =
    deriveEnumerationReader[TimeProviderType]
  implicit val timeProviderTypeWriter: ConfigWriter[TimeProviderType] =
    deriveEnumerationWriter[TimeProviderType]

  implicit val dbConfigSynchronousCommitValueReader: ConfigReader[SynchronousCommitValue] =
    deriveEnumerationReader[SynchronousCommitValue]
  implicit val dbConfigSynchronousCommitValueWriter: ConfigWriter[SynchronousCommitValue] =
    deriveEnumerationWriter[SynchronousCommitValue]

  implicit val dbConfigConnectionPoolConfigReader: ConfigReader[ConnectionPoolConfig] =
    deriveReader[ConnectionPoolConfig]
  implicit val dbConfigConnectionPoolConfigWriter: ConfigWriter[ConnectionPoolConfig] =
    deriveWriter[ConnectionPoolConfig]

  implicit val dbConfigPostgresDataSourceConfigReader: ConfigReader[PostgresDataSourceConfig] =
    deriveReader[PostgresDataSourceConfig]
  implicit val dbConfigPostgresDataSourceConfigWriter: ConfigWriter[PostgresDataSourceConfig] =
    deriveWriter[PostgresDataSourceConfig]

  implicit val dbConfigReader: ConfigReader[DbConfig] = deriveReader[DbConfig]
  implicit val dbConfigWriter: ConfigWriter[DbConfig] = deriveWriter[DbConfig]

  implicit val apiServerConfigReader: ConfigReader[ApiServerConfig] = deriveReader[ApiServerConfig]
  implicit val apiServerConfigWriter: ConfigWriter[ApiServerConfig] = deriveWriter[ApiServerConfig]

  implicit val participantRunModeReader: ConfigReader[ParticipantRunMode] =
    deriveEnumerationReader[ParticipantRunMode]
  implicit val participantRunModeWriter: ConfigWriter[ParticipantRunMode] =
    deriveEnumerationWriter[ParticipantRunMode]

  implicit val validateAndStartReader: ConfigReader[IndexerStartupMode.ValidateAndStart.type] =
    deriveReader[IndexerStartupMode.ValidateAndStart.type]
  implicit val validateAndStartWriter: ConfigWriter[IndexerStartupMode.ValidateAndStart.type] =
    deriveWriter[IndexerStartupMode.ValidateAndStart.type]

  implicit val MigrateOnEmptySchemaAndStartReader
      : ConfigReader[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type] =
    deriveReader[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type]
  implicit val MigrateOnEmptySchemaAndStartWriter
      : ConfigWriter[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type] =
    deriveWriter[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type]

  implicit val migrateAndStartReader: ConfigReader[IndexerStartupMode.MigrateAndStart] =
    deriveReader[IndexerStartupMode.MigrateAndStart]
  implicit val migrateAndStartWriter: ConfigWriter[IndexerStartupMode.MigrateAndStart] =
    deriveWriter[IndexerStartupMode.MigrateAndStart]

  implicit val validateAndWaitOnlyReader: ConfigReader[IndexerStartupMode.ValidateAndWaitOnly] =
    deriveReader[IndexerStartupMode.ValidateAndWaitOnly]
  implicit val validateAndWaitOnlyWriter: ConfigWriter[IndexerStartupMode.ValidateAndWaitOnly] =
    deriveWriter[IndexerStartupMode.ValidateAndWaitOnly]

  implicit val indexerStartupModeReader: ConfigReader[IndexerStartupMode] =
    deriveReader[IndexerStartupMode]
  implicit val indexerStartupModeWriter: ConfigWriter[IndexerStartupMode] =
    deriveWriter[IndexerStartupMode]

  implicit val haConfigReader: ConfigReader[HaConfig] = deriveReader[HaConfig]
  implicit val haConfigWriter: ConfigWriter[HaConfig] = deriveWriter[HaConfig]

  private def createParticipantId(participantId: String) =
    Ref.ParticipantId
      .fromString(participantId)
      .left
      .map(err => CannotConvert(participantId, Ref.ParticipantId.getClass.getName, err))

  implicit val participantIdReader: ConfigReader[Ref.ParticipantId] = ConfigReader
    .fromString[Ref.ParticipantId](createParticipantId)

  implicit val participantIdWriter: ConfigWriter[Ref.ParticipantId] =
    ConfigWriter.toString[Ref.ParticipantId](_.toString)

  implicit val indexerConfigReader: ConfigReader[IndexerConfig] = deriveReader[IndexerConfig]
  implicit val indexerConfigWriter: ConfigWriter[IndexerConfig] = deriveWriter[IndexerConfig]

  implicit val sizedCacheReader: ConfigReader[SizedCache.Configuration] =
    deriveReader[SizedCache.Configuration]
  implicit val sizedCacheWriter: ConfigWriter[SizedCache.Configuration] =
    deriveWriter[SizedCache.Configuration]

  implicit val lfValueTranslationCacheReader: ConfigReader[LfValueTranslationCache.Config] =
    deriveReader[LfValueTranslationCache.Config]
  implicit val lfValueTranslationCacheWriter: ConfigWriter[LfValueTranslationCache.Config] =
    deriveWriter[LfValueTranslationCache.Config]

  implicit val indexConfigurationReader: ConfigReader[IndexConfiguration] =
    deriveReader[IndexConfiguration]
  implicit val indexConfigurationWriter: ConfigWriter[IndexConfiguration] =
    deriveWriter[IndexConfiguration]

  implicit val participantConfigReader: ConfigReader[ParticipantConfig] =
    deriveReader[ParticipantConfig]
  implicit val participantConfigWriter: ConfigWriter[ParticipantConfig] =
    deriveWriter[ParticipantConfig]

  implicit def participantNameKeyReader: ConfigReader[Map[ParticipantName, ParticipantConfig]] =
    genericMapReader[ParticipantName, ParticipantConfig]((s: String) => Right(ParticipantName(s)))
  implicit def participantNameKeyWriter: ConfigWriter[Map[ParticipantName, ParticipantConfig]] =
    genericMapWriter[ParticipantName, ParticipantConfig](_.value)

  implicit val reader: ConfigReader[Config] = deriveReader[Config]
  implicit val writer: ConfigWriter[Config] = deriveWriter[Config]
}
