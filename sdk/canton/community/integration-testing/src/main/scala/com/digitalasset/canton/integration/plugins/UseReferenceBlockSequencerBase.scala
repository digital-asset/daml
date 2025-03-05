// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  MultiSynchronizer,
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.integration.{EnvironmentSetupPlugin, TestConsoleEnvironment}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig

/** @param sequencerGroups
  *   If sequencerGroups is defined, all the sequencers of the same set will share the same storage
  *   (which means they are part of the same synchronizer). All sequencers in the config that are
  *   not in a defined group will be placed in the same default group. If it is not defined, all
  *   sequencers will share the same storage by default and one synchronizer only is assumed. If
  *   in-memory storage is defined, sequencers sharing storage is not supported (each one is a
  *   different synchronizer).
  */
abstract class UseReferenceBlockSequencerBase[
    StorageConfigT <: StorageConfig,
    SequencerConfigT <: SequencerConfig,
    EnvT <: Environment,
    TestConsoleEnvT <: TestConsoleEnvironment[EnvT],
](
    override protected val loggerFactory: NamedLoggerFactory,
    driverSingleWordName: String,
    sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
) extends EnvironmentSetupPlugin[EnvT, TestConsoleEnvT] {

  protected final def dbNameForGroup(group: Int): String = s"${driverSingleWordName}_db_$group"

  protected val dbNames: NonEmpty[List[String]] = sequencerGroups match {
    case SingleSynchronizer => NonEmpty(List, dbNameForGroup(0))
    case MultiSynchronizer(_) =>
      NonEmpty(
        List,
        dbNameForGroup(0), // db 0 is the default one
        (1 to sequencerGroups.numberOfSynchronizers).map(i => dbNameForGroup(i)).toList *,
      )
  }

  protected def driverConfigs(
      config: CantonConfig,
      storageConfigs: Map[InstanceName, StorageConfigT],
  ): Map[InstanceName, SequencerConfigT]
}

object UseReferenceBlockSequencerBase {

  sealed trait SequencerSynchronizerGroups {
    def numberOfSynchronizers: Int
  }

  case object SingleSynchronizer extends SequencerSynchronizerGroups {
    override val numberOfSynchronizers: Int = 1
  }

  final case class MultiSynchronizer(sequencerGroups: Seq[Set[InstanceName]])
      extends SequencerSynchronizerGroups {
    override val numberOfSynchronizers: Int = sequencerGroups.size
  }

  object MultiSynchronizer {
    def tryCreate(sequencerGroups: Set[String]*): MultiSynchronizer =
      MultiSynchronizer(sequencerGroups.map(_.map(InstanceName.tryCreate)))
  }
}
