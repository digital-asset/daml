// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.tests.sequencer.reference.ReferenceSequencerDriverApiConformanceTestBase.DummyCantonConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencing.SequencerDriverApiConformanceTest

abstract class ReferenceSequencerDriverApiConformanceTestBase[ConfigType]
    extends SequencerDriverApiConformanceTest[ConfigType] {

  protected def plugin: EnvironmentSetupPlugin

  protected def parseConfig(
      maybeConfig: Option[SequencerConfig]
  ): ConfigType

  override def beforeAll(): Unit = {
    super.beforeAll()
    val config = plugin.beforeEnvironmentCreated(DummyCantonConfig)
    val dConfig = parseConfig(config.sequencers.headOption.map(_._2.sequencer))
    plugin.beforeTests()
    driverConfig.set(Some(dConfig))
  }

  override def afterAll(): Unit = {
    plugin.afterEnvironmentDestroyed(DummyCantonConfig)
    plugin.afterTests()
    driverConfig.set(None)
    super.afterAll()
  }
}

object ReferenceSequencerDriverApiConformanceTestBase {

  private[sequencer] val DummyCantonConfig = new CantonConfig(
    sequencers = Map(InstanceName.tryCreate("sequencer") -> SequencerNodeConfig())
  )
}
