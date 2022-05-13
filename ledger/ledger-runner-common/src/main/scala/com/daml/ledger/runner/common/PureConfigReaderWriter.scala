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
  IndexServiceConfig,
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
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter, ConvertHelpers}

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

  implicit val interpretationLimitsConvert: ConfigConvert[interpretation.Limits] =
    deriveConvert[interpretation.Limits]

  implicit val contractKeyUniquenessModeConvert: ConfigConvert[ContractKeyUniquenessMode] =
    deriveEnumerationConvert[ContractKeyUniquenessMode]

  implicit val engineConvert: ConfigConvert[EngineConfig] = deriveConvert[EngineConfig]

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

  implicit val metricsConvert: ConfigConvert[MetricsConfig] = deriveConvert[MetricsConfig]

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

  implicit val tlsConfigurationConvert: ConfigConvert[TlsConfiguration] =
    deriveConvert[TlsConfiguration]

  implicit val portReader: ConfigReader[Port] = ConfigReader.intConfigReader.map(Port.apply)
  implicit val portWriter: ConfigWriter[Port] = ConfigWriter.intConfigWriter.contramap[Port] {
    port: Port => port.value
  }

  implicit val initialLedgerConfigurationConvert: ConfigConvert[InitialLedgerConfiguration] =
    deriveConvert[InitialLedgerConfiguration]

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

  implicit val userManagementConfigConvert: ConfigConvert[UserManagementConfig] =
    deriveConvert[UserManagementConfig]

  implicit val authServiceConfigUnsafeJwtHmac256Reader
      : ConfigConvert[AuthServiceConfig.UnsafeJwtHmac256] =
    deriveConvert[AuthServiceConfig.UnsafeJwtHmac256]
  implicit val authServiceConfigJwtEs256CrtConvert: ConfigConvert[AuthServiceConfig.JwtEs256Crt] =
    deriveConvert[AuthServiceConfig.JwtEs256Crt]
  implicit val authServiceConfigJwtEs512CrtConvert: ConfigConvert[AuthServiceConfig.JwtEs512Crt] =
    deriveConvert[AuthServiceConfig.JwtEs512Crt]
  implicit val authServiceConfigJwtRs256CrtConvert: ConfigConvert[AuthServiceConfig.JwtRs256Crt] =
    deriveConvert[AuthServiceConfig.JwtRs256Crt]
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

  implicit val dbConfigConvert: ConfigConvert[DbConfig] = deriveConvert[DbConfig]

  implicit val apiServerConfigConvert: ConfigConvert[ApiServerConfig] =
    deriveConvert[ApiServerConfig]

  implicit val participantRunModeConvert: ConfigConvert[ParticipantRunMode] =
    deriveEnumerationConvert[ParticipantRunMode]

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

  implicit val indexerConfigConvert: ConfigConvert[IndexerConfig] = deriveConvert[IndexerConfig]

  implicit val sizedCacheConvert: ConfigConvert[SizedCache.Configuration] =
    deriveConvert[SizedCache.Configuration]

  implicit val lfValueTranslationCacheConvert: ConfigConvert[LfValueTranslationCache.Config] =
    deriveConvert[LfValueTranslationCache.Config]

  implicit val indexConfigurationConvert: ConfigConvert[IndexServiceConfig] =
    deriveConvert[IndexServiceConfig]

  implicit val participantConfigConvert: ConfigConvert[ParticipantConfig] =
    deriveConvert[ParticipantConfig]

  implicit val participantNameKeyReader: ConfigReader[Map[ParticipantName, ParticipantConfig]] =
    genericMapReader[ParticipantName, ParticipantConfig]((s: String) => Right(ParticipantName(s)))
  implicit val participantNameKeyWriter: ConfigWriter[Map[ParticipantName, ParticipantConfig]] =
    genericMapWriter[ParticipantName, ParticipantConfig](_.value)

  implicit val convert: ConfigConvert[Config] = deriveConvert[Config]
}
