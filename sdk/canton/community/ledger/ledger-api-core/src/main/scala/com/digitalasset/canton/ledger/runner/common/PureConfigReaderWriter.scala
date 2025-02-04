// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.jwt.JwtTimestampLeeway
import com.daml.ports.Port
import com.digitalasset.canton.ledger.runner.common.OptConfigValue.{
  optConvertEnabled,
  optProductHint,
}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  CommandServiceConfig,
  IdentityProviderManagementConfig,
  IndexServiceConfig,
  PartyManagementServiceConfig,
  TransactionTreeStreamsConfig,
  UpdatesStreamsConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.digitalasset.daml.lf.data.Ref
import pureconfig.configurable.{genericMapReader, genericMapWriter}
import pureconfig.error.CannotConvert
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.*
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

class PureConfigReaderWriter(secure: Boolean = true) {

  private val ReplaceSecretWithString = "<REDACTED>"

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

  implicit val portReader: ConfigReader[Port] = ConfigReader.intConfigReader.map(Port.apply)
  implicit val portWriter: ConfigWriter[Port] = ConfigWriter.intConfigWriter.contramap[Port] {
    _.value
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
            s"Seeding is neither ${Seeding.Strong.name}, ${Seeding.Weak.name}, nor ${Seeding.Static.name}: $unknownSeeding",
          )
        )
    }

  implicit val seedingWriter: ConfigWriter[Seeding] = ConfigWriter.toString(_.name)

  implicit val userManagementServiceConfigHint: ProductHint[UserManagementServiceConfig] =
    ProductHint[UserManagementServiceConfig](allowUnknownKeys = false)

  implicit val userManagementServiceConfigConvert: ConfigConvert[UserManagementServiceConfig] =
    deriveConvert[UserManagementServiceConfig]

  implicit val partyManagementServiceConfigHint: ProductHint[PartyManagementServiceConfig] =
    ProductHint[PartyManagementServiceConfig](allowUnknownKeys = false)

  implicit val partyManagementServiceConfigConvert: ConfigConvert[PartyManagementServiceConfig] =
    deriveConvert[PartyManagementServiceConfig]

  implicit val identityProviderManagementConfigHint: ProductHint[IdentityProviderManagementConfig] =
    ProductHint[IdentityProviderManagementConfig](allowUnknownKeys = false)

  implicit val identityProviderManagementConfigConvert
      : ConfigConvert[IdentityProviderManagementConfig] =
    deriveConvert[IdentityProviderManagementConfig]

  implicit val jwtTimestampLeewayConfigHint: OptConfigValue.OptProductHint[JwtTimestampLeeway] =
    optProductHint[JwtTimestampLeeway](allowUnknownKeys = false)

  implicit val jwtTimestampLeewayConfigConvert: ConfigConvert[Option[JwtTimestampLeeway]] =
    optConvertEnabled(deriveConvert[JwtTimestampLeeway])

  implicit val commandConfigurationHint: ProductHint[CommandServiceConfig] =
    ProductHint[CommandServiceConfig](allowUnknownKeys = false)

  implicit val commandConfigurationConvert: ConfigConvert[CommandServiceConfig] =
    deriveConvert[CommandServiceConfig]

  implicit val dbConfigSynchronousCommitValueConvert: ConfigConvert[SynchronousCommitValue] =
    deriveEnumerationConvert[SynchronousCommitValue]

  implicit val dbConfigConnectionPoolConfigHint: ProductHint[ConnectionPoolConfig] =
    ProductHint[ConnectionPoolConfig](allowUnknownKeys = false)

  implicit val dbConfigConnectionPoolConfigConvert: ConfigConvert[ConnectionPoolConfig] =
    deriveConvert[ConnectionPoolConfig]

  implicit val dbConfigPostgresDataSourceConfigHint: ProductHint[PostgresDataSourceConfig] =
    ProductHint[PostgresDataSourceConfig](allowUnknownKeys = false)

  implicit val dbConfigPostgresDataSourceConfigConvert: ConfigConvert[PostgresDataSourceConfig] =
    deriveConvert[PostgresDataSourceConfig]

  implicit val dataSourcePropertiesHint: ProductHint[DataSourceProperties] =
    ProductHint[DataSourceProperties](allowUnknownKeys = false)

  implicit val dataSourcePropertiesConvert: ConfigConvert[DataSourceProperties] =
    deriveConvert[DataSourceProperties]

  implicit val rateLimitingConfigHint: OptConfigValue.OptProductHint[RateLimitingConfig] =
    optProductHint[RateLimitingConfig](allowUnknownKeys = false)

  implicit val rateLimitingConfigConvert: ConfigConvert[Option[RateLimitingConfig]] =
    optConvertEnabled(deriveConvert[RateLimitingConfig])

  implicit val haConfigHint: ProductHint[HaConfig] =
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
    ConfigWriter.toString[Ref.ParticipantId](identity)

  implicit val indexerConfigHint: ProductHint[IndexerConfig] =
    ProductHint[IndexerConfig](allowUnknownKeys = false)

  implicit val indexerConfigConvert: ConfigConvert[IndexerConfig] = deriveConvert[IndexerConfig]

  implicit val indexServiceConfigHint: ProductHint[IndexServiceConfig] =
    ProductHint[IndexServiceConfig](allowUnknownKeys = false)

  implicit val activecContractsServiceStreamsConfigConvert
      : ConfigConvert[ActiveContractsServiceStreamsConfig] =
    deriveConvert[ActiveContractsServiceStreamsConfig]

  implicit val transactionTreeStreamsConfigConvert: ConfigConvert[TransactionTreeStreamsConfig] =
    deriveConvert[TransactionTreeStreamsConfig]

  implicit val transactionFlatStreamsConfigConvert: ConfigConvert[UpdatesStreamsConfig] =
    deriveConvert[UpdatesStreamsConfig]

  implicit val indexServiceConfigConvert: ConfigConvert[IndexServiceConfig] =
    deriveConvert[IndexServiceConfig]

  implicit val participantDataSourceConfigReader: ConfigReader[ParticipantDataSourceConfig] =
    ConfigReader.fromString[ParticipantDataSourceConfig] { url =>
      Right(ParticipantDataSourceConfig(url))
    }

  implicit val participantDataSourceConfigWriter: ConfigWriter[ParticipantDataSourceConfig] =
    ConfigWriter.toString {
      case _ if secure => ReplaceSecretWithString
      case dataSourceConfig => dataSourceConfig.jdbcUrl
    }

  implicit val participantDataSourceConfigMapReader
      : ConfigReader[Map[Ref.ParticipantId, ParticipantDataSourceConfig]] =
    genericMapReader[Ref.ParticipantId, ParticipantDataSourceConfig]((s: String) =>
      createParticipantId(s)
    )
  implicit val participantDataSourceConfigMapWriter
      : ConfigWriter[Map[Ref.ParticipantId, ParticipantDataSourceConfig]] =
    genericMapWriter[Ref.ParticipantId, ParticipantDataSourceConfig](identity)

}

object PureConfigReaderWriter {
  implicit val Secure: PureConfigReaderWriter = new PureConfigReaderWriter(secure = true)
}
