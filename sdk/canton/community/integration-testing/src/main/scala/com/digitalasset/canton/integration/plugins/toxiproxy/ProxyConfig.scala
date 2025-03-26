// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins.toxiproxy

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.plugins.toxiproxy.ProxyConfig.*
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Assertions.fail

import scala.jdk.CollectionConverters.*

sealed trait ProxyConfig {
  def name: String
  def generate(config: CantonConfig): ProxyInstanceConfig
}

final case class ParticipantToSequencerPublicApi(sequencer: String, name: String)
    extends ProxyConfig {
  override def generate(config: CantonConfig): ProxyInstanceConfig = {
    val sequencerConfig =
      config.sequencersByString.getOrElse(
        sequencer,
        throw new RuntimeException(s"Cannot get config for sequencer $sequencer"),
      )
    BasicProxyInstanceConfig(
      name,
      sequencerConfig.publicApi.address,
      sequencerConfig.publicApi.port,
      this,
    )
  }
}

final case class ParticipantToAwsKms(name: String, participant: String) extends ProxyConfig {
  override def generate(config: CantonConfig): ProxyInstanceConfig = {
    val configValue = config.participantsByString
      .withDefault(config.participantsByString.getOrElse(_, fail(s"No config for $participant")))
      .apply(participant)

    val crypto = configValue.crypto
    val region = crypto.kms.toList
      .collectFirst { case aws: KmsConfig.Aws =>
        aws.region
      }
      .getOrElse(fail(s"Participant $participant is not configured with AWS KMS"))

    crypto.provider match {
      case CryptoProvider.Kms =>
        ParticipantAwsKmsInstanceConfig(
          name,
          s"kms.$region.amazonaws.com",
          Port.tryCreate(443),
          this,
        )
      case _ => throw new RuntimeException(s"Participant $participant is not using an AWS KMS")
    }

  }
}

final case class ParticipantToGcpKms(name: String, participant: String) extends ProxyConfig {
  override def generate(config: CantonConfig): ProxyInstanceConfig = {
    val configValue = config.participantsByString
      .withDefault(config.participantsByString.getOrElse(_, fail(s"No config for $participant")))
      .apply(participant)

    val crypto = configValue.crypto
    crypto.provider match {
      case CryptoProvider.Kms =>
        ParticipantGcpKmsInstanceConfig(
          name,
          "cloudkms.googleapis.com",
          Port.tryCreate(443),
          this,
        )
      case _ => throw new RuntimeException(s"Participant $participant is not using a GCP KMS")
    }

  }
}

final case class ParticipantToPostgres(
    name: String,
    participant: String,
    dbTimeouts: Long = 10000L,
) extends ProxyConfig {
  override def generate(config: CantonConfig): ProxyInstanceConfig =
    participantStorage(config, participant) match {
      case postgres: DbConfig.Postgres =>
        val (host: String, port: Port, dbName: String) = dbConfig(postgres.config)
        ParticipantPostgresInstanceConfig(name, host, port, this, dbName, postgres, dbTimeouts)

      case x =>
        throw new RuntimeException(
          s"Participant $participant is not using Postgres, instead uses $x"
        )
    }
}

final case class MediatorToPostgres(name: String, mediator: String, dbTimeout: Long = 10000L)
    extends ProxyConfig {
  override def generate(config: CantonConfig): ProxyInstanceConfig =
    mediatorStorage(config, mediator) match {
      case postgres: DbConfig.Postgres =>
        val (host: String, port: Port, dbName: String) = dbConfig(postgres.config)
        MediatorPostgresInstanceConfig(name, host, port, this, dbName, postgres, dbTimeout)

      case x =>
        throw new RuntimeException(
          s"Mediator $mediator is not using Postgres, instead uses $x"
        )
    }
}

final case class SequencerToPostgres(name: String, sequencer: String, dbTimeout: Long = 10000L)
    extends ProxyConfig {
  override def generate(config: CantonConfig): ProxyInstanceConfig =
    sequencerStorage(config, sequencer) match {
      case postgres: DbConfig.Postgres =>
        val (host: String, port: Port, dbName: String) = dbConfig(postgres.config)
        SequencerPostgresInstanceConfig(name, host, port, this, dbName, postgres, dbTimeout)

      case x =>
        throw new RuntimeException(s"Sequencer $sequencer is not using Postgres, instead uses $x")
    }
}

object ProxyConfig {
  private def storageConfig[N <: LocalNodeConfig](
      name: String,
      nodes: Map[String, N],
      nodeType: String,
  ): StorageConfig =
    nodes
      .get(name)
      .fold(
        fail(
          s"No config option for $nodeType $name. Only found ${nodeType}s ${nodes.keySet.mkString(", ")}."
        )
      )(_.storage)

  def participantStorage(config: CantonConfig, participant: String): StorageConfig =
    storageConfig(participant, config.participantsByString, "participant")

  def sequencerStorage(config: CantonConfig, sequencer: String): StorageConfig =
    storageConfig(sequencer, config.sequencersByString, "sequencer")

  def mediatorStorage(config: CantonConfig, mediator: String): StorageConfig =
    storageConfig(mediator, config.mediatorsByString, "mediator")

  def dbConfig(config: Config): (String, Port, String) =
    if (config.hasPath("url")) {
      val url = config.getValue("url")
      // look for patterns such as //localhost:5432/this_is_my_db_name
      val hostPortDbnameM =
        """(.*@|//)(.+):(\d+)/(.+)$""".r
          .findFirstMatchIn(url.render())
          .getOrElse(
            throw new RuntimeException(s"Expected to find a host, port and dbname in $url")
          )

      val host = hostPortDbnameM.group(2)
      val port = Port.tryCreate(hostPortDbnameM.group(3).toInt)
      val dbName = {
        val name = hostPortDbnameM.group(4)
        if (name.indexOf("\"") == name.length - 1) name.dropRight(1)
        else name
      }
      (host, port, dbName)
    } else {
      val host = config.getString("properties.serverName")
      val port = Port.tryCreate(config.getInt("properties.portNumber"))
      val dbName = config.getString("properties.databaseName")
      (host, port, dbName)
    }

  def postgresConfig(
      dbName: String,
      proxy: RunningProxy,
      timeout: Long,
      postgres: DbConfig.Postgres,
  ): DbConfig.Postgres = {
    val basicConfig = postgres.config
    val custom =
      ConfigFactory.parseMap(
        Map[String, Any](
          "connectionTimeout" -> timeout,
          "properties.serverName" -> proxy.ipFromHost,
          "properties.portNumber" -> proxy.portFromHost,
          "properties.databaseName" -> dbName,
        ).asJava
      )
    val newConfig = custom.withFallback(basicConfig).resolve()
    postgres.copy(
      config = newConfig,
      parameters =
        postgres.parameters.copy(connectionTimeout = NonNegativeFiniteDuration.ofMillis(timeout)),
    )
  }
}
