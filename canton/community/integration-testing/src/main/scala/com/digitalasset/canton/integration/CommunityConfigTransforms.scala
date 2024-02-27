// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.syntax.option.*
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.{
  CantonCommunityConfig,
  CommunityDbConfig,
  CommunityStorageConfig,
  H2DbConfig,
  StorageConfig,
}
import com.digitalasset.canton.domain.config.CommunityDomainConfig
import com.digitalasset.canton.participant.config.CommunityParticipantConfig
import com.digitalasset.canton.version.{DomainProtocolVersion, ProtocolVersion}
import com.typesafe.config.{Config, ConfigValueFactory}
import monocle.macros.syntax.lens.*

import scala.reflect.ClassTag
import scala.util.Random

object CommunityConfigTransforms {

  type CommunityConfigTransform = CantonCommunityConfig => CantonCommunityConfig

  /** Parameterized version to allow specifying community or enterprise versions */
  def withUniqueDbName[SC <: StorageConfig, H2SC <: H2DbConfig with SC](
      nodeName: String,
      storageConfig: SC,
      mkH2: Config => H2SC,
  )(implicit h2Tag: ClassTag[H2SC]): SC =
    storageConfig match {
      case h2: H2SC =>
        // Make sure that each environment and its database names are unique by generating a random prefix
        val dbName = generateUniqueH2DatabaseName(nodeName)
        mkH2(
          h2.config.withValue(
            "url",
            ConfigValueFactory.fromAnyRef(
              s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;LOCK_TIMEOUT=10000;DB_CLOSE_DELAY=-1"
            ),
          )
        )
      case x => x
    }

  def ammoniteWithoutConflicts: CommunityConfigTransform =
    config =>
      config
        .focus(_.parameters.console.cacheDir)
        .replace(None) // don't use cache for testing

  def withUniqueDbName(
      nodeName: String,
      storageConfig: CommunityStorageConfig,
  ): CommunityStorageConfig =
    withUniqueDbName(nodeName, storageConfig, CommunityDbConfig.H2(_))

  def generateUniqueH2DatabaseName(nodeName: String): String = {
    val dbPrefix = Random.alphanumeric.take(8).map(_.toLower).mkString
    s"${dbPrefix}_$nodeName"
  }

  def updateAllDomainsProtocolVersion(pv: ProtocolVersion): CommunityConfigTransform =
    updateAllDomainConfigs_(
      _.focus(_.init.domainParameters.protocolVersion).replace(DomainProtocolVersion(pv))
    )

  def updateAllDomainConfigs_(
      update: CommunityDomainConfig => CommunityDomainConfig
  ): CommunityConfigTransform =
    updateAllDomainConfigs((_, config) => update(config))

  def updateAllDomainConfigs(
      update: (String, CommunityDomainConfig) => CommunityDomainConfig
  ): CommunityConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.domains)
        .modify(_.map { case (dName, dConfig) => (dName, update(dName.unwrap, dConfig)) })

  def updateAllParticipantConfigs(
      update: (String, CommunityParticipantConfig) => CommunityParticipantConfig
  ): CommunityConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.participants)
        .modify(_.map { case (pName, pConfig) => (pName, update(pName.unwrap, pConfig)) })

  def uniqueH2DatabaseNames: CommunityConfigTransform = {
    updateAllDomainConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    } compose updateAllParticipantConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    }
  }

  def uniquePorts: CommunityConfigTransform = {

    def nextPort = UniquePortGenerator.next

    val domainUpdate = updateAllDomainConfigs { case (_, config) =>
      config
        .focus(_.publicApi.internalPort)
        .replace(nextPort.some)
        .focus(_.adminApi.internalPort)
        .replace(nextPort.some)
    }

    val participantUpdate = updateAllParticipantConfigs { case (_, config) =>
      config
        .focus(_.ledgerApi.internalPort)
        .replace(nextPort.some)
        .focus(_.adminApi.internalPort)
        .replace(nextPort.some)
    }

    domainUpdate compose participantUpdate

  }
}
