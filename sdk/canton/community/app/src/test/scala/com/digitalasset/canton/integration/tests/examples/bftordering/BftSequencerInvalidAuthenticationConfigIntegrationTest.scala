// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples.bftordering

import better.files.StringExtensions
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality

class BftSequencerInvalidAuthenticationConfigIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  registerPlugin(new UseBftSequencer(loggerFactory, shouldGenerateEndpointsOnly = true))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.fromFiles(
      "community" / "app" / "src" / "test" / "resources" / "invalid-configs" / "bft-sequencer-misconfigured-authentication.conf"
    )

  "sequencers" should {
    "fail on authentication" in { implicit env =>
      import env.*
      val synchronizerAlias = "bft-sequencer-invalid-config-test"
      val participant = lp("participant1")
      val mediator = lm("mediator1")
      val sequencer1 = ls("sequencer1")
      val sequencer2 = ls("sequencer2")

      loggerFactory.assertLogsUnorderedOptional(
        {
          bootstrap.synchronizer(
            synchronizerAlias,
            sequencers = sequencers.all,
            mediators = mediators.all,
            synchronizerOwners = sequencers.all,
            synchronizerThreshold = PositiveInt.two,
            staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
          )

          the[CommandFailure] thrownBy
            participant.synchronizers.connect_local(sequencer1, synchronizerAlias)

          // Close within log suppression to avoid flakiness
          mediator.stop()
          sequencer1.stop()
          sequencer2.stop()
        },
        (
          LogEntryOptionality.Required,
          _.warningMessage should include("Insecure setup"),
        ),
        (
          LogEntryOptionality.Optional,
          _.warningMessage should include("Token refresh aborted due to shutdown"),
        ),
        (
          LogEntryOptionality.OptionalMany,
          _.warningMessage should include("Token refresh encountered error"),
        ),
        (
          LogEntryOptionality.OptionalMany,
          _.warningMessage should include("Authentication headers are not set"),
        ),
        (
          LogEntryOptionality.OptionalMany,
          _.warningMessage should include("failed to ping endpoint"),
        ),
        (
          LogEntryOptionality.Optional,
          _.errorMessage should include("Request failed for participant1"),
        ),
      )
    }
  }
}
