// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.commands.ParticipantCommands
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.{DomainAlias, SequencerAlias}

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
      """If vetAllPackages is true, the participants will vet the package on all domains they are registered.
        If synchronizeVetting is true, the command will block until the package vetting transaction has been registered with all connected domains."""
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
  }

  @Help.Summary("Manage domain connections on several participants at once")
  @Help.Group("Domains")
  object domains extends Helpful {

    @Help.Summary("Disconnect from domain")
    def disconnect(alias: DomainAlias): Unit =
      ConsoleCommandResult
        .runAll(participants)(ParticipantCommands.domains.disconnect(_, alias))
        .discard

    @Help.Summary("Reconnect to domain")
    @Help.Description(
      "If retry is set to true (default), the command will return after the first attempt, but keep on trying in the background."
    )
    def reconnect(alias: DomainAlias, retry: Boolean = true): Unit =
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.domains.reconnect(_, alias, retry)
        )
        .discard

    @Help.Summary("Reconnect to all domains for which `manualStart` = false")
    @Help.Description(
      """If ignoreFailures is set to true (default), the reconnect all will succeed even if some domains are offline.
          | The participants will continue attempting to establish a domain connection."""
    )
    def reconnect_all(ignoreFailures: Boolean = true): Unit = {
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.domains.reconnect_all(_, ignoreFailures = ignoreFailures)
        )
        .discard
    }

    @Help.Summary("Disconnect from all connected domains")
    def disconnect_all(): Unit =
      ConsoleCommandResult
        .runAll(participants) { p =>
          ConsoleCommandResult.fromEither(for {
            connected <- ParticipantCommands.domains.list_connected(p).toEither
            _ <- connected
              .traverse(d => ParticipantCommands.domains.disconnect(p, d.domainAlias).toEither)
          } yield ())
        }
        .discard

    @Help.Summary("Register and potentially connect to domain")
    def register(
        config: DomainConnectionConfig,
        handshakeOnly: Boolean = false,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit =
      ConsoleCommandResult
        .runAll(participants)(
          ParticipantCommands.domains.register(_, config, handshakeOnly = handshakeOnly, validation)
        )
        .discard

    @Help.Summary("Register and potentially connect to new local domain")
    @Help.Description("""
        The arguments are:
          domain - A local domain or sequencer reference
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_local(
        domain: SequencerNodeReference,
        alias: DomainAlias,
        manualConnect: Boolean = false,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      val config =
        ParticipantCommands.domains.reference_to_config(
          NonEmpty.mk(Seq, SequencerAlias.Default -> domain).toMap,
          alias,
          manualConnect,
        )
      register(config)
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
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
