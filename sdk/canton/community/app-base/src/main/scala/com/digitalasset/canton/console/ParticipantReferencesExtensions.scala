// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.commands.ParticipantCommands
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}

class ParticipantReferencesExtensions(participants: Seq[ParticipantReference])(implicit
    override val consoleEnvironment: ConsoleEnvironment
) extends Helpful
    with NamedLogging
    with FeatureFlagFilter {

  protected override def loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory

  @Help.Summary("Manage dars on several participants at once")
  @Help.Group("DAR Management")
  object dars extends Helpful {
    @Help.Summary("Upload DARs to participants")
    @Help.Description(
      """If vetAllPackages is true, the participants will vet the package on all synchronizers they are registered.
        If synchronizeVetting is true, the command will block until the package vetting transaction has been registered with all connected synchronizers."""
    )
    def upload(
        darPath: String,
        vetAllPackages: Boolean = true,
        synchronizeVetting: Boolean = true,
    ): Map[ParticipantReference, String] = {
      val res = ConsoleCommandResult.runAll(participants)(
        ParticipantCommands.dars
          .upload(
            _,
            darPath,
            vetAllPackages = vetAllPackages,
            synchronizeVetting = synchronizeVetting,
            logger,
          )
      )
      if (synchronizeVetting && vetAllPackages) {
        participants.foreach(_.packages.synchronize_vetting())
      }
      res
    }

    @Help.Summary("Validate DARs against the current participants' state")
    @Help.Description(
      """Performs the same DAR and Daml package validation checks that the upload call performs,
         but with no effects on the target participants: the DAR is not persisted or vetted."""
    )
    def validate(darPath: String): Map[ParticipantReference, String] =
      ConsoleCommandResult.runAll(participants)(
        ParticipantCommands.dars
          .validate(
            _,
            darPath,
            logger,
          )
      )
  }

  @Help.Summary("Manage synchronizer connections on several participants at once")
  @Help.Group("Synchronizers")
  object synchronizers extends Helpful {

    @Help.Summary("Disconnect from synchronizer")
    def disconnect(alias: SynchronizerAlias): Unit =
      ConsoleCommandResult
        .runAll(participants)(ParticipantCommands.synchronizers.disconnect(_, alias))
        .discard

    @Help.Summary("Disconnect from all connected synchronizers")
    def disconnect_all(): Unit =
      ConsoleCommandResult
        .runAll(participants) { p =>
          ConsoleCommandResult.fromEither(
            ParticipantCommands.synchronizers.disconnect_all(p).toEither
          )
        }
        .discard

    @Help.Summary("Reconnect to synchronizer")
    @Help.Description(
      "If retry is set to true (default), the command will return after the first attempt, but keep on trying in the background."
    )
    def reconnect(alias: SynchronizerAlias, retry: Boolean = true): Unit =
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.synchronizers.reconnect(_, alias, retry)
        )
        .discard

    @Help.Summary("Reconnect to all synchronizers for which `manualStart` = false")
    @Help.Description(
      """If ignoreFailures is set to true (default), the reconnect all will succeed even if some synchronizers are offline.
          | The participants will continue attempting to establish a synchronizer connection."""
    )
    def reconnect_all(ignoreFailures: Boolean = true): Unit =
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.synchronizers.reconnect_all(_, ignoreFailures = ignoreFailures)
        )
        .discard

    @Help.Summary("Register a synchronizer")
    def register(
        config: SynchronizerConnectionConfig,
        performHandshake: Boolean = true,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit =
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.synchronizers
            .register(_, config, performHandshake = performHandshake, validation)
        )
        .discard

    @Help.Summary("Connect to a domain")
    def connect(
        config: SynchronizerConnectionConfig,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit =
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.synchronizers.connect(_, config, validation)
        )
        .discard

    @Help.Summary("Register and potentially connect to new local domain")
    @Help.Description("""
        The arguments are:
          synchronizer - A local synchronizer or sequencer reference
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_local(
        sequencer: SequencerReference,
        alias: SynchronizerAlias,
        manualConnect: Boolean = false,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      val config =
        ParticipantCommands.synchronizers.reference_to_config(
          NonEmpty.mk(Seq, SequencerAlias.Default -> sequencer).toMap,
          alias,
          manualConnect,
        )

      connect(config)
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))
      }
    }
  }

}

class LocalParticipantReferencesExtensions(
    participants: Seq[LocalParticipantReference]
)(implicit
    override val consoleEnvironment: ConsoleEnvironment
) extends ParticipantReferencesExtensions(participants)
    with LocalInstancesExtensions[LocalParticipantReference] {
  override def instances: Seq[LocalParticipantReference] = participants
}
