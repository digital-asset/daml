// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.jwt.JwtTimestampLeeway
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.{LanguageDevConfig, LanguageVersion}
import com.daml.lf.transaction.ContractKeyUniquenessMode
import com.daml.lf.{VersionRange, interpretation, language}
import com.daml.metrics.api.reporters.MetricsReporter
import com.daml.ports.Port
import com.digitalasset.canton.ledger.api.tls.TlsVersion.TlsVersion
import com.digitalasset.canton.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.digitalasset.canton.ledger.runner.common.OptConfigValue.{
  optConvertEnabled,
  optProductHint,
}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  CommandServiceConfig,
  IndexServiceConfig,
  TransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig,
  IndexerStartupMode,
  PackageMetadataViewConfig,
}
import com.digitalasset.canton.platform.localstore.IdentityProviderManagementConfig
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import io.netty.handler.ssl.ClientAuth
import pureconfig.configurable.{genericMapReader, genericMapWriter}
import pureconfig.error.CannotConvert
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.*
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter, ConvertHelpers}

import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Try

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
class PureConfigReaderWriter(secure: Boolean = true) {
  val Secret = "<REDACTED>"

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
        value.split("-") match {
          case Array(min, max) =>
            val convertedValue: Either[String, VersionRange[LanguageVersion]] = for {
              min <- language.LanguageVersion.fromString(min)
              max <- language.LanguageVersion.fromString(max)
            } yield {
              VersionRange[language.LanguageVersion](min, max)
            }
            convertedValue.left.map { error =>
              CannotConvert(
                value,
                VersionRange.getClass.getName,
                s"$value is not recognized. " + error,
              )
            }
          case _ =>
            Left(
              CannotConvert(value, VersionRange.getClass.getName, s"$value is not recognized. ")
            )
        }
    }

  implicit val versionRangeWriter: ConfigWriter[VersionRange[language.LanguageVersion]] =
    ConfigWriter.toString {
      case LanguageVersion.DevVersions => "daml-lf-dev-mode-unsafe"
      case LanguageVersion.EarlyAccessVersions => "early-access"
      case LanguageVersion.StableVersions => "stable"
      case LanguageVersion.LegacyVersions => "legacy"
      case range => s"${range.min.pretty}-${range.max.pretty}"
    }

  implicit val interpretationLimitsHint =
    ProductHint[interpretation.Limits](allowUnknownKeys = false)

  implicit val interpretationLimitsConvert: ConfigConvert[interpretation.Limits] =
    deriveConvert[interpretation.Limits]

  implicit val contractKeyUniquenessModeConvert: ConfigConvert[ContractKeyUniquenessMode] =
    deriveEnumerationConvert[ContractKeyUniquenessMode]

  implicit val evaluationOrderReader: ConfigReader[LanguageDevConfig.EvaluationOrder] =
    ConfigReader.fromString[LanguageDevConfig.EvaluationOrder] {
      case "left-to-right" => Right(LanguageDevConfig.LeftToRight)
      case "right-to-left" => Right(LanguageDevConfig.RightToLeft)
      case value =>
        Left(
          CannotConvert(
            value,
            LanguageDevConfig.EvaluationOrder.getClass.getName,
            s"$value is not recognized. ",
          )
        )
    }

  implicit val evaluationOrderWriter: ConfigWriter[LanguageDevConfig.EvaluationOrder] =
    ConfigWriter.toString {
      case LanguageDevConfig.LeftToRight => "left-to-right"
      case LanguageDevConfig.RightToLeft => "right-to-left"
    }

  implicit val engineHint = ProductHint[EngineConfig](allowUnknownKeys = false)

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

  implicit val tlsConfigurationHint = ProductHint[TlsConfiguration](allowUnknownKeys = false)

  implicit val tlsConfigurationConvert: ConfigConvert[TlsConfiguration] =
    deriveConvert[TlsConfiguration]

  implicit val portReader: ConfigReader[Port] = ConfigReader.intConfigReader.map(Port.apply)
  implicit val portWriter: ConfigWriter[Port] = ConfigWriter.intConfigWriter.contramap[Port] {
    port: Port => port.value
  }

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

  implicit val userManagementServiceConfigHint =
    ProductHint[UserManagementServiceConfig](allowUnknownKeys = false)

  implicit val userManagementServiceConfigConvert: ConfigConvert[UserManagementServiceConfig] =
    deriveConvert[UserManagementServiceConfig]

  implicit val identityProviderManagementConfigHint =
    ProductHint[IdentityProviderManagementConfig](allowUnknownKeys = false)

  implicit val identityProviderManagementConfigConvert
      : ConfigConvert[IdentityProviderManagementConfig] =
    deriveConvert[IdentityProviderManagementConfig]

  implicit val jwtTimestampLeewayConfigHint: OptConfigValue.OptProductHint[JwtTimestampLeeway] =
    optProductHint[JwtTimestampLeeway](allowUnknownKeys = false)

  implicit val jwtTimestampLeewayConfigConvert: ConfigConvert[Option[JwtTimestampLeeway]] =
    optConvertEnabled(deriveConvert[JwtTimestampLeeway])

  implicit val commandConfigurationHint =
    ProductHint[CommandServiceConfig](allowUnknownKeys = false)

  implicit val commandConfigurationConvert: ConfigConvert[CommandServiceConfig] =
    deriveConvert[CommandServiceConfig]

  implicit val timeProviderTypeConvert: ConfigConvert[TimeProviderType] =
    deriveEnumerationConvert[TimeProviderType]

  implicit val dbConfigSynchronousCommitValueConvert: ConfigConvert[SynchronousCommitValue] =
    deriveEnumerationConvert[SynchronousCommitValue]

  implicit val dbConfigConnectionPoolConfigHint =
    ProductHint[ConnectionPoolConfig](allowUnknownKeys = false)

  implicit val dbConfigConnectionPoolConfigConvert: ConfigConvert[ConnectionPoolConfig] =
    deriveConvert[ConnectionPoolConfig]

  implicit val dbConfigPostgresDataSourceConfigHint =
    ProductHint[PostgresDataSourceConfig](allowUnknownKeys = false)

  implicit val dbConfigPostgresDataSourceConfigConvert: ConfigConvert[PostgresDataSourceConfig] =
    deriveConvert[PostgresDataSourceConfig]

  implicit val dataSourcePropertiesHint =
    ProductHint[DataSourceProperties](allowUnknownKeys = false)

  implicit val dataSourcePropertiesConvert: ConfigConvert[DataSourceProperties] =
    deriveConvert[DataSourceProperties]

  implicit val rateLimitingConfigHint: OptConfigValue.OptProductHint[RateLimitingConfig] =
    optProductHint[RateLimitingConfig](allowUnknownKeys = false)

  implicit val rateLimitingConfigConvert: ConfigConvert[Option[RateLimitingConfig]] =
    optConvertEnabled(deriveConvert[RateLimitingConfig])

  implicit val validateAndStartConvert: ConfigConvert[IndexerStartupMode.ValidateAndStart.type] =
    deriveConvert[IndexerStartupMode.ValidateAndStart.type]

  implicit val MigrateOnEmptySchemaAndStartReader
      : ConfigConvert[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type] =
    deriveConvert[IndexerStartupMode.MigrateOnEmptySchemaAndStart.type]

  implicit val migrateAndStartConvert: ConfigConvert[IndexerStartupMode.MigrateAndStart.type] =
    deriveConvert[IndexerStartupMode.MigrateAndStart.type]

  implicit val validateAndWaitOnlyHint =
    ProductHint[IndexerStartupMode.ValidateAndWaitOnly](allowUnknownKeys = false)

  implicit val validateAndWaitOnlyConvert: ConfigConvert[IndexerStartupMode.ValidateAndWaitOnly] =
    deriveConvert[IndexerStartupMode.ValidateAndWaitOnly]

  implicit val indexerStartupModeConvert: ConfigConvert[IndexerStartupMode] =
    deriveConvert[IndexerStartupMode]

  implicit val haConfigHint =
    ProductHint[HaConfig](allowUnknownKeys = false)

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

  implicit val packageMetadataViewConfigHint =
    ProductHint[PackageMetadataViewConfig](allowUnknownKeys = false)

  implicit val packageMetadataViewConfigConvert: ConfigConvert[PackageMetadataViewConfig] =
    deriveConvert[PackageMetadataViewConfig]

  implicit val indexerConfigHint =
    ProductHint[IndexerConfig](allowUnknownKeys = false)

  implicit val indexerConfigConvert: ConfigConvert[IndexerConfig] = deriveConvert[IndexerConfig]

  implicit val indexServiceConfigHint =
    ProductHint[IndexServiceConfig](allowUnknownKeys = false)

  implicit val activecContractsServiceStreamsConfigConvert
      : ConfigConvert[ActiveContractsServiceStreamsConfig] =
    deriveConvert[ActiveContractsServiceStreamsConfig]

  implicit val transactionTreeStreamsConfigConvert: ConfigConvert[TransactionTreeStreamsConfig] =
    deriveConvert[TransactionTreeStreamsConfig]

  implicit val transactionFlatStreamsConfigConvert: ConfigConvert[TransactionFlatStreamsConfig] =
    deriveConvert[TransactionFlatStreamsConfig]

  implicit val indexServiceConfigConvert: ConfigConvert[IndexServiceConfig] =
    deriveConvert[IndexServiceConfig]

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

}

object PureConfigReaderWriter {
  implicit val Secure = new PureConfigReaderWriter(secure = true)
}
