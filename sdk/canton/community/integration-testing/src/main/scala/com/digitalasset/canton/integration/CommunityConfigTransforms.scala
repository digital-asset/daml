// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.syntax.option.*
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.{CantonCommunityConfig, DbConfig, StorageConfig}
import com.digitalasset.canton.participant.config.CommunityParticipantConfig
import com.digitalasset.canton.synchronizer.mediator.CommunityMediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.config.CommunitySequencerNodeConfig
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import com.typesafe.config.{Config, ConfigValueFactory}
import monocle.macros.syntax.lens.*

import scala.util.Random

object CommunityConfigTransforms {

  type CommunityConfigTransform = CantonCommunityConfig => CantonCommunityConfig

  /** Parameterized version to allow specifying community or enterprise versions */
  def withUniqueDbName[SC <: StorageConfig](
      nodeName: String,
      storageConfig: SC,
      mkH2: Config => SC,
  ): SC =
    storageConfig match {
      case h2: DbConfig.H2 =>
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
      storageConfig: StorageConfig,
  ): StorageConfig =
    withUniqueDbName(nodeName, storageConfig, DbConfig.H2(_))

  def generateUniqueH2DatabaseName(nodeName: String): String = {
    val dbPrefix = Random.alphanumeric.take(8).map(_.toLower).mkString
    s"${dbPrefix}_$nodeName"
  }

  def updateAllParticipantConfigs(
      update: (String, CommunityParticipantConfig) => CommunityParticipantConfig
  ): CommunityConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.participants)
        .modify(_.map { case (pName, pConfig) => (pName, update(pName.unwrap, pConfig)) })

  def updateAllSequencerConfigs(
      update: (String, CommunitySequencerNodeConfig) => CommunitySequencerNodeConfig
  ): CommunityConfigTransform =
    _.focus(_.sequencers)
      .modify(_.map { case (sName, sConfig) => (sName, update(sName.unwrap, sConfig)) })

  def updateAllSequencerConfigs_(
      update: CommunitySequencerNodeConfig => CommunitySequencerNodeConfig
  ): CommunityConfigTransform =
    updateAllSequencerConfigs((_, config) => update(config))

  def updateAllMediatorConfigs_(
      update: CommunityMediatorNodeConfig => CommunityMediatorNodeConfig
  ): CommunityConfigTransform =
    updateAllMediatorConfigs((_, config) => update(config))

  def updateAllMediatorConfigs(
      update: (String, CommunityMediatorNodeConfig) => CommunityMediatorNodeConfig
  ): CommunityConfigTransform =
    cantonConfig =>
      cantonConfig
        .focus(_.mediators)
        .modify(_.map { case (pName, pConfig) => (pName, update(pName.unwrap, pConfig)) })

  def uniqueH2DatabaseNames: CommunityConfigTransform =
    updateAllSequencerConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    } compose updateAllMediatorConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    } compose updateAllParticipantConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    }

  def setNonStandardConfig(enable: Boolean): CommunityConfigTransform =
    _.focus(_.parameters.nonStandardConfig).replace(enable)

  def setGlobalAlphaVersionSupport(enable: Boolean): CommunityConfigTransform =
    _.focus(_.parameters.alphaVersionSupport).replace(enable)

  def setGlobalBetaVersionSupport(enable: Boolean): CommunityConfigTransform =
    _.focus(_.parameters.betaVersionSupport).replace(enable)

  def updateAllParticipantConfigs_(
      update: CommunityParticipantConfig => CommunityParticipantConfig
  ): CommunityConfigTransform =
    updateAllParticipantConfigs((_, participantConfig) => update(participantConfig))

  def setAlphaVersionSupport(enable: Boolean): Seq[CommunityConfigTransform] = Seq(
    setNonStandardConfig(enable),
    setGlobalAlphaVersionSupport(enable),
    updateAllParticipantConfigs_(
      _.focus(_.parameters.alphaVersionSupport)
        .replace(enable)
    ),
  )

  def setBetaVersionSupport(enable: Boolean): Seq[CommunityConfigTransform] =
    Seq(
      setGlobalBetaVersionSupport(enable),
      updateAllParticipantConfigs_(
        _.focus(_.parameters.betaVersionSupport)
          .replace(enable)
      ),
    )

  lazy val enableAlphaVersionSupport: Seq[CommunityConfigTransform] = setAlphaVersionSupport(true)
  lazy val enableBetaVersionSupport: Seq[CommunityConfigTransform] = setBetaVersionSupport(true)

  lazy val dontWarnOnDeprecatedPV = Seq(
    updateAllSequencerConfigs_(
      _.focus(_.parameters.dontWarnOnDeprecatedPV).replace(true)
    ),
    updateAllMediatorConfigs_(
      _.focus(_.parameters.dontWarnOnDeprecatedPV).replace(true)
    ),
    updateAllParticipantConfigs_(
      _.focus(_.parameters.dontWarnOnDeprecatedPV).replace(true)
    ),
  )

  def updateAllInitialProtocolVersion(pv: ProtocolVersion): Seq[CommunityConfigTransform] = Seq(
    updateAllParticipantConfigs_(
      _.focus(_.parameters.initialProtocolVersion).replace(ParticipantProtocolVersion(pv))
    )
  )

  def setProtocolVersion(pv: ProtocolVersion): Seq[CommunityConfigTransform] = {
    def configTransformsWhen(predicate: Boolean)(transforms: => Seq[CommunityConfigTransform]) =
      if (predicate) transforms else Seq()

    val enableAlpha = configTransformsWhen(pv.isAlpha)(enableAlphaVersionSupport)
    val enableBeta = configTransformsWhen(pv.isBeta)(enableBetaVersionSupport)

    val deprecatedPVWarning = configTransformsWhen(pv.isDeprecated)(dontWarnOnDeprecatedPV)

    val updateParticipants = Seq(
      updateAllParticipantConfigs_(
        _.focus(_.parameters.minimumProtocolVersion)
          .replace(Some(ParticipantProtocolVersion(pv)))
      )
    )

    val updateInitialProtocolVersion = updateAllInitialProtocolVersion(pv)

    updateParticipants ++ enableAlpha ++ enableBeta ++ deprecatedPVWarning ++ updateInitialProtocolVersion
  }

  def uniquePorts: CommunityConfigTransform = {

    def nextPort = UniquePortGenerator.next

    val participantUpdate = updateAllParticipantConfigs { case (_, config) =>
      config
        .focus(_.ledgerApi.internalPort)
        .replace(nextPort.some)
        .focus(_.adminApi.internalPort)
        .replace(nextPort.some)
    }

    val sequencerUpdate = updateAllSequencerConfigs_(
      _.focus(_.publicApi.internalPort)
        .replace(nextPort.some)
        .focus(_.adminApi.internalPort)
        .replace(nextPort.some)
        .focus(_.monitoring.grpcHealthServer)
        .modify(_.map(_.copy(internalPort = nextPort.some)))
    )

    val mediatorUpdate = updateAllMediatorConfigs_(
      _.focus(_.adminApi.internalPort)
        .replace(nextPort.some)
        .focus(_.monitoring.grpcHealthServer)
        .modify(_.map(_.copy(internalPort = nextPort.some)))
    )

    participantUpdate compose sequencerUpdate compose mediatorUpdate
  }
}
