// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.NamedLoggerFactory
import monocle.macros.syntax.lens.*

/** Plugin to override the auth configuration to allow everything.
  */
final case class NoAuthPlugin(
    protected val loggerFactory: NamedLoggerFactory
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(
      config: CantonConfig
  ): CantonConfig =
    ConfigTransforms
      .updateParticipantConfig("participant1")(
        _.focus(_.ledgerApi.authServices).replace(Seq(Wildcard))
      )(config)
}
