// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{
  CantonCommunityConfig,
  CantonRequireTypes,
  CommunityDbConfig,
  CommunityStorageConfig,
  DbParametersConfig,
}
import com.digitalasset.canton.environment.CommunityEnvironment
import com.digitalasset.canton.integration.CommunityConfigTransforms.generateUniqueH2DatabaseName
import com.digitalasset.canton.integration.CommunityTests.CommunityTestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  MultiDomain,
  SequencerDomainGroups,
  SingleDomain,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.synchronizer.sequencing.config.CommunitySequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  CommunityReferenceSequencerDriverFactory,
  ReferenceSequencerDriver,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.{
  BlockSequencerConfig,
  CommunitySequencerConfig,
}
import com.digitalasset.canton.util.ErrorUtil
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
    ](loggerFactory, "reference", sequencerGroups) {

  private val driverFactory = new CommunityReferenceSequencerDriverFactory

  override def driverConfigs(
      config: CantonCommunityConfig,
      storageConfigs: Map[CantonRequireTypes.InstanceName, S],
  ): Map[InstanceName, CommunitySequencerConfig] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.forClass(loggerFactory, classOf[UseCommunityReferenceBlockSequencer[S]])
    config.sequencers.keys.map { sequencerName =>
      sequencerName -> CommunitySequencerConfig.External(
        driverFactory.name,
        BlockSequencerConfig(),
        ConfigCursor(
          driverFactory
            .configWriter(confidential = false)
            .to(
              ReferenceSequencerDriver
                .Config(
                  storageConfigs.getOrElse(
                    sequencerName,
                    ErrorUtil.invalidState(s"Missing storage config for $sequencerName"),
                  )
                )
            ),
          List(),
        ),
      )
    }.toMap
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def beforeEnvironmentCreated(config: CantonCommunityConfig): CantonCommunityConfig = {
    // in H2 we need to make db name unique, but it has to be matching for all sequencers, so we cache it
    lazy val dbNamesH2: Map[String, String] = dbNames.forgetNE.map { dbName =>
      dbName -> generateUniqueH2DatabaseName(dbName)
    }.toMap

    def dbToStorageConfig(dbName: String, dbParametersConfig: DbParametersConfig): S =
      c.runtimeClass match {
        case cl if cl == classOf[CommunityDbConfig.H2] =>
          val h2DbName = dbNamesH2.getOrElse(
            dbName,
            throw new IllegalStateException(
              s"Impossible code path: dbName $dbName not found in $dbNamesH2"
            ),
          )
          DbBasicConfig("user", "pass", h2DbName, "", 0).toH2DbConfig
            .copy(parameters = dbParametersConfig)
            .asInstanceOf[S]
        case cl if cl == classOf[canton.config.CommunityStorageConfig.Memory] =>
          CommunityStorageConfig.Memory(parameters = dbParametersConfig).asInstanceOf[S]
        case other =>
          // E.g. Nothing; we need to check and fail b/c the Scala compiler doesn't enforce
          //  passing the ClassTag-reified type parameter, if it's only used for a ClassTag implicit
          sys.error(
            s"The reference sequencer driver doesn't recognize storage type $other"
          )
      }

    val storageConfigMap: Map[InstanceName, S] = sequencerGroups match {
      case SingleDomain =>
        config.sequencers.map { case (name, sequencerConfig) =>
          val dbParameters = sequencerConfig.storage.parameters
          (name, dbToStorageConfig(dbNames.head1, dbParameters))
        }
      case MultiDomain(groups) =>
        groups.zipWithIndex.flatMap { case (sequencers, i) =>
          val dbName = dbNameForGroup(i + 1)
          sequencers.map { name =>
            val dbParameters = config.sequencers
              .get(name)
              .map(
                _.storage.parameters
              )
              .getOrElse(DbParametersConfig())

            (name, dbToStorageConfig(dbName, dbParameters))
          }
        }.toMap
    }

    val sequencersToConfig: Map[InstanceName, CommunitySequencerConfig] =
      driverConfigs(config, storageConfigMap)

    def mapSequencerConfigs(
        kv: (InstanceName, CommunitySequencerNodeConfig)
    ): (InstanceName, CommunitySequencerNodeConfig) = kv match {
      case (name, cfg) =>
        (
          name,
          cfg.focus(_.sequencer).replace(sequencersToConfig(name)),
        )
    }

    config
      .focus(_.sequencers)
      .modify(_.map(mapSequencerConfigs))
  }
}
