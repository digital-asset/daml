// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.*
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig

class SequencerDriverConfigTest extends BaseTestWordSpec {
  lazy val baseDir: File = "community" / "app" / "src" / "test" / "resources"
  lazy val dummySequencerConfigFile: File = baseDir / "dummy-sequencer-config.conf"
  val driverName = "dummy"
  val driverFactory = new DummyDriverFactory
  val password = "mypass"

  "sequencer driver configs" should {
    "load sequencer config and be able to parse driver specific config" in {
      val sequencerConfig = {
        val configE =
          CantonConfig.parseAndLoad(
            Seq(dummySequencerConfigFile.toJava),
            Some(DefaultPorts.create()),
          )
        val cantonConfig = valueOrFail(configE)("loading dummy sequencer config")
        valueOrFail(
          cantonConfig.sequencers.get(InstanceName.tryCreate("dummy-sequencer"))
        )("retrieving dummy sequencer config")
      }
      sequencerConfig.sequencer match {
        case SequencerConfig.External(`driverName`, _, configCursor) =>
          val config =
            valueOrFail(driverFactory.configParser.from(configCursor))("parsing driver config")
          config shouldBe (DummyDriverFactory.DummyConfig(Password(password), "something"))
        case _ => fail("expected an external sequencer config")
      }
    }
    "respect confidentiality of passwords when writing" in {
      val configE =
        CantonConfig.parseAndLoad(
          Seq(dummySequencerConfigFile.toJava),
          Some(DefaultPorts.create()),
        )
      val cantonConfig = valueOrFail(configE)("loading dummy sequencer config")
      val confString = CantonConfig.makeConfidentialString(cantonConfig)
      confString should not include (password)
    }
  }
}
