// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.*
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerConfig
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  SequencerDomainGroups,
  SingleDomain,
}
import com.digitalasset.canton.integration.{EnvironmentSetupPlugin, TestConsoleEnvironment}
import com.digitalasset.canton.logging.NamedLoggerFactory

/** @param sequencerGroups If sequencerGroups is defined, all the sequencers of the same set will share the same storage
  *                        (which means they are part of the same domain).
  *                        All sequencers in the config that are not in a defined group will be placed in the same default group.
  *                        If it is not defined, all sequencers will share the same storage by default and one domain only is assumed.
  *                        If in-memory storage is defined, sequencers sharing storage is not supported (each one is a different domain).
  */
abstract class UseReferenceBlockSequencerBase[
    StorageConfigT <: StorageConfig,
    SequencerConfigT <: SequencerConfig,
    EnvT <: Environment,
    TestConsoleEnvT <: TestConsoleEnvironment[EnvT],
](
    override protected val loggerFactory: NamedLoggerFactory,
    driverSingleWordName: String,
    sequencerGroups: SequencerDomainGroups = SingleDomain,
) extends EnvironmentSetupPlugin[EnvT, TestConsoleEnvT] {

  protected final def dbNameForGroup(group: Int): String = s"${driverSingleWordName}_db_$group"

  protected val dbNames: NonEmpty[List[String]] = NonEmpty(
    List,
    dbNameForGroup(0), // db 0 is the default one
    (1 to sequencerGroups.numberOfDomains).map(i => dbNameForGroup(i)).toList *,
  )

  protected def driverConfigs(
      config: EnvT#Config,
      defaultStorageConfig: StorageConfigT,
      storageConfigs: Map[InstanceName, StorageConfigT],
  ): Map[InstanceName, SequencerConfigT]
}

object UseReferenceBlockSequencerBase {

  sealed trait SequencerDomainGroups {
    def numberOfDomains: Int
  }

  case object SingleDomain extends SequencerDomainGroups {
    override val numberOfDomains: Int = 1
  }

  final case class MultiDomain(sequencerGroups: Seq[Set[InstanceName]])
      extends SequencerDomainGroups {
    override val numberOfDomains: Int = sequencerGroups.size
  }

  object MultiDomain {
    def tryCreate(sequencerGroups: Set[String]*): MultiDomain =
      MultiDomain(sequencerGroups.map(_.map(InstanceName.tryCreate)))
  }
}
