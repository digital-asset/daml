// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.integration.{ConfigTransform, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.NamedLoggerFactory

/** Apply a set of config transforms as part of the plugin execution, such that we can apply a
  * config transform also after a specific plugin has been executed.
  */
class UseConfigTransforms(
    configTransforms: Seq[ConfigTransform],
    override protected val loggerFactory: NamedLoggerFactory,
) extends EnvironmentSetupPlugin {

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    configTransforms.foldLeft(config)((cfg, transform) => transform(cfg))
}
