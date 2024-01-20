// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{
  CantonCommunityConfig,
  CantonRequireTypes,
  CommunityDbConfig,
  CommunityStorageConfig,
}
import com.digitalasset.canton.domain.sequencing.config.CommunitySequencerNodeXConfig
import com.digitalasset.canton.domain.sequencing.sequencer.CommunitySequencerConfig
import com.digitalasset.canton.domain.sequencing.sequencer.reference.{
  CommunityReferenceSequencerDriverFactory,
  ReferenceBlockOrderer,
}
import com.digitalasset.canton.environment.CommunityEnvironment
import com.digitalasset.canton.integration.CommunityConfigTransforms
import com.digitalasset.canton.integration.CommunityTests.CommunityTestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  MultiDomain,
  SequencerDomainGroups,
  SingleDomain,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import monocle.macros.syntax.lens.*
import pureconfig.ConfigCursor

import scala.reflect.ClassTag

class UseCommunityReferenceBlockSequencer[S <: CommunityStorageConfig](
    override protected val loggerFactory: NamedLoggerFactory,
    sequencerGroups: SequencerDomainGroups = SingleDomain,
)(implicit c: ClassTag[S])
    extends UseReferenceBlockSequencerBase[
      S,
      CommunitySequencerConfig,
      CommunityEnvironment,
      CommunityTestConsoleEnvironment,
    ](loggerFactory, "reference", "reference", sequencerGroups) {

  private val driverFactory = new CommunityReferenceSequencerDriverFactory

  override def driverConfigs(
      config: CantonCommunityConfig,
      defaultStorageConfig: S,
      storageConfigs: Map[CantonRequireTypes.InstanceName, S],
  ): Map[InstanceName, CommunitySequencerConfig] =
    config.sequencers.keys.map { sequencerName =>
      sequencerName -> CommunitySequencerConfig.External(
        driverFactory.name,
        ConfigCursor(
          driverFactory
            .configWriter(confidential = false)
            .to(
              ReferenceBlockOrderer
                .Config(storageConfigs.getOrElse(sequencerName, defaultStorageConfig))
            ),
          List(),
        ),
        None,
      )
    }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def beforeEnvironmentCreated(config: CantonCommunityConfig): CantonCommunityConfig = {
    val dbToStorageConfig: Map[String, S] =
      c.runtimeClass match {
        case cl if cl == classOf[CommunityDbConfig.H2] =>
          dbNames
            .map(db =>
              (
                db,
                CommunityDbConfig.H2(
                  CommunityConfigTransforms
                    .withUniqueDbName(
                      db,
                      DbBasicConfig("user", "pass", db, "", 0).toH2DbConfig,
                      CommunityDbConfig.H2(_),
                    )
                    .config
                ),
              )
            )
            .forgetNE
            .toMap
            .asInstanceOf[Map[String, S]]
        case cl if cl == classOf[CommunityStorageConfig.Memory] =>
          dbNames.forgetNE
            .map(db => (db, CommunityStorageConfig.Memory()))
            .toMap
            .asInstanceOf[Map[String, S]]
        case other =>
          // E.g. Nothing; we need to check and fail b/c the Scala compiler doesn't enforce
          //  passing the ClassTag-reified type parameter, if it's only used for a ClassTag implicit
          sys.error(
            s"The reference sequencer driver doesn't recognize and/or support storage type $other"
          )
      }

    lazy val defaultDriverConfig: S = {
      val defaultDbName = dbNames.head1
      dbToStorageConfig(defaultDbName)
    }

    val storageConfigMap: Map[InstanceName, S] = sequencerGroups match {
      case MultiDomain(groups) =>
        groups.zipWithIndex.flatMap { case (sequencers, i) =>
          val dbName = dbNameForGroup(i + 1)
          sequencers.map(name => (name, dbToStorageConfig(dbName)))
        }.toMap
      case _ => Map.empty
    }

    val sequencersToConfig: Map[InstanceName, CommunitySequencerConfig] =
      driverConfigs(config, defaultDriverConfig, storageConfigMap)

    def mapSequencerXConfigs(
        kv: (InstanceName, CommunitySequencerNodeXConfig)
    ): (InstanceName, CommunitySequencerNodeXConfig) = kv match {
      case (name, config) =>
        (
          name,
          config.focus(_.sequencer).replace(sequencersToConfig(name)),
        )
    }

    config
      .focus(_.sequencers)
      .modify(_.map(mapSequencerXConfigs))
  }
}
