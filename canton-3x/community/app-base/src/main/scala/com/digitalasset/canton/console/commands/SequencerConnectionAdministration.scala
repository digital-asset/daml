// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerConnectionAdminCommands
import com.digitalasset.canton.console.{AdminCommandRunner, Help, Helpful, InstanceReferenceCommon}
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}

import scala.util.Try

trait SequencerConnectionAdministration extends Helpful {
  this: AdminCommandRunner with InstanceReferenceCommon =>

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
        EnterpriseSequencerConnectionAdminCommands.GetConnection()
      )
    }

    @Help.Summary("Set Sequencer Connection")
    @Help.Description(
      "Set new sequencer connection details for this sequencer client node. " +
        "This will replace any pre-configured connection details. " +
        "This command will only work after the node has been initialized."
    )
    def set(connections: SequencerConnections): Unit = consoleEnvironment.run {
      adminCommand(
        EnterpriseSequencerConnectionAdminCommands.SetConnection(connections)
      )
    }

    @Help.Summary("Set Sequencer Connection")
    @Help.Description(
      "Set new sequencer connection details for this sequencer client node. " +
        "This will replace any pre-configured connection details. " +
        "This command will only work after the node has been initialized."
    )
    def set(connection: SequencerConnection): Unit = consoleEnvironment.run {
      adminCommand(
        EnterpriseSequencerConnectionAdminCommands.SetConnection(
          SequencerConnections.single(connection)
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
        modifier: SequencerConnections => SequencerConnections
    ): Unit =
      consoleEnvironment.runE {
        for {
          connOption <- adminCommand(
            EnterpriseSequencerConnectionAdminCommands.GetConnection()
          ).toEither
          conn <- connOption.toRight("Node not yet initialized")
          newConn <- Try(modifier(conn)).toEither.leftMap(_.getMessage)
          _ <- adminCommand(
            EnterpriseSequencerConnectionAdminCommands.SetConnection(newConn)
          ).toEither

        } yield ()
      }

  }
}
