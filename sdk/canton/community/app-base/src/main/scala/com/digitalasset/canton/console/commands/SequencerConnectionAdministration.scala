// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.commands.SequencerConnectionAdminCommands
import com.digitalasset.canton.console.{AdminCommandRunner, Help, Helpful, InstanceReference}
import com.digitalasset.canton.sequencing.{
  SequencerConnection,
  SequencerConnectionValidation,
  SequencerConnections,
}

import scala.util.Try

trait SequencerConnectionAdministration extends Helpful {
  this: AdminCommandRunner with InstanceReference =>

  @Help.Summary("Manage sequencer connection")
  @Help.Group("Sequencer Connection")
  object sequencer_connection extends Helpful {
    @Help.Summary("Get Sequencer Connection")
    @Help.Description(
      "Use this command to get the currently configured sequencer connection details for this sequencer client. " +
        "If this node has not yet been initialized, this will return None."
    )
    def get(): Option[SequencerConnections] = consoleEnvironment.run {
      adminCommand(
        SequencerConnectionAdminCommands.GetConnection()
      )
    }

    @Help.Summary("Set Sequencer Connection")
    @Help.Description(
      "Set new sequencer connection details for this sequencer client node. " +
        "This will replace any pre-configured connection details. " +
        "This command will only work after the node has been initialized."
    )
    def set(
        connections: SequencerConnections,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit = consoleEnvironment.run {
      adminCommand(
        SequencerConnectionAdminCommands.SetConnection(connections, validation)
      )
    }

    @Help.Summary("Set Sequencer Connection")
    @Help.Description(
      "Set new sequencer connection details for this sequencer client node. " +
        "This will replace any pre-configured connection details. " +
        "This command will only work after the node has been initialized."
    )
    def set_single(
        connection: SequencerConnection,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit = consoleEnvironment.run {
      adminCommand(
        SequencerConnectionAdminCommands.SetConnection(
          SequencerConnections.single(connection),
          validation,
        )
      )
    }

    @Help.Summary("Modify Default Sequencer Connection")
    @Help.Description(
      "Modify sequencer connection details for this sequencer client node, " +
        "by passing a modifier function that operates on the existing default connection. "
    )
    def modify(modifier: SequencerConnection => SequencerConnection): Unit =
      modify_connections(_.modify(SequencerAlias.Default, modifier))

    @Help.Summary("Modify Sequencer Connections")
    @Help.Description(
      "Modify sequencer connection details for this sequencer client node, " +
        "by passing a modifier function that operates on the existing connection configuration. "
    )
    def modify_connections(
        modifier: SequencerConnections => SequencerConnections,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit =
      consoleEnvironment.runE {
        for {
          connOption <- adminCommand(
            SequencerConnectionAdminCommands.GetConnection()
          ).toEither
          conn <- connOption.toRight("Node not yet initialized")
          newConn <- Try(modifier(conn)).toEither.leftMap(_.getMessage)
          _ <- adminCommand(
            SequencerConnectionAdminCommands.SetConnection(newConn, validation)
          ).toEither

        } yield ()
      }

    @Help.Summary(
      "Revoke this sequencer client node's authentication tokens and close all the sequencers connections."
    )
    @Help.Description("""
      On all the sequencers, all existing authentication tokens for this sequencer client node will be revoked.
      Note that the node is not disconnected from the synchronizer; only the connections to the sequencers are closed.
      The node will automatically reopen connections, perform a challenge-response and obtain new tokens.
      """)
    def logout(): Unit = consoleEnvironment.run {
      adminCommand(
        SequencerConnectionAdminCommands.Logout()
      )
    }

  }
}
