// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, TopologyAdminCommands}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  Help,
  Helpful,
  InstanceReferenceCommon,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*

import java.util.concurrent.atomic.AtomicReference

// TODO(#15161): fold TopologyAdministrationGroupCommon into TopologyAdministrationX
abstract class TopologyAdministrationGroupCommon(
    instance: InstanceReferenceCommon,
    topologyQueueStatus: => Option[TopologyQueueStatus],
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends Helpful {

  protected val runner: AdminCommandRunner = instance

  def owner_to_key_mappings: OwnerToKeyMappingsGroup

  // small cache to avoid repetitive calls to fetchId (as the id is immutable once set)
  protected val idCache =
    new AtomicReference[Option[UniqueIdentifier]](None)

  private[console] def clearCache(): Unit = {
    idCache.set(None)
  }

  protected def getIdCommand(): ConsoleCommandResult[UniqueIdentifier]

  private[console] def idHelper[T](
      apply: UniqueIdentifier => T
  ): T = {
    apply(idCache.get() match {
      case Some(v) => v
      case None =>
        val r = consoleEnvironment.run {
          getIdCommand()
        }
        idCache.set(Some(r))
        r
    })
  }

  @Help.Summary("Topology synchronisation helpers", FeatureFlag.Preview)
  @Help.Group("Synchronisation Helpers")
  object synchronisation {

    @Help.Summary("Check if the topology processing of a node is idle")
    @Help.Description("""Topology transactions pass through a set of queues before becoming effective on a domain.
        |This function allows to check if all the queues are empty.
        |While both domain and participant nodes support similar queues, there is some ambiguity around
        |the participant queues. While the domain does really know about all in-flight transactions at any
        |point in time, a participant won't know about the state of any transaction that is currently being processed
        |by the domain topology dispatcher.""")
    def is_idle(): Boolean =
      topologyQueueStatus
        .forall(_.isIdle) // report un-initialised as idle to not break manual init process

    @Help.Summary("Wait until the topology processing of a node is idle")
    @Help.Description("""This function waits until the `is_idle()` function returns true.""")
    def await_idle(
        timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
    ): Unit =
      ConsoleMacros.utils.retry_until_true(timeout)(
        is_idle(),
        s"topology queue status never became idle ${topologyQueueStatus} after ${timeout}",
      )

    /** run a topology change command synchronized and wait until the node becomes idle again */
    private[console] def run[T](timeout: Option[NonNegativeDuration])(func: => T): T = {
      val ret = func
      ConsoleMacros.utils.synchronize_topology(timeout)(consoleEnvironment)
      ret
    }

    /** run a topology change command synchronized and wait until the node becomes idle again */
    private[console] def runAdminCommand[T](
        timeout: Option[NonNegativeDuration]
    )(grpcCommand: => GrpcAdminCommand[_, _, T]): T = {
      val ret = consoleEnvironment.run(runner.adminCommand(grpcCommand))
      // Only wait for topology synchronization if a timeout is specified.
      if (timeout.nonEmpty) {
        ConsoleMacros.utils.synchronize_topology(timeout)(consoleEnvironment)
      }
      ret
    }
  }

}

/** OwnerToKeyMappingsGroup to parameterize by different TopologyChangeOp/X
  */
abstract class OwnerToKeyMappingsGroup(
    commandTimeouts: ConsoleCommandTimeout
) {
  def rotate_key(
      nodeInstance: InstanceReferenceCommon,
      owner: Member,
      currentKey: PublicKey,
      newKey: PublicKey,
      synchronize: Option[config.NonNegativeDuration] = Some(commandTimeouts.bounded),
  ): Unit
}

trait InitNodeId extends ConsoleCommandGroup {

  @Help.Summary("Initialize the node with a unique identifier")
  @Help.Description("""Every node in Canton is identified using a unique identifier, which is composed
                      |of a user-chosen string and the fingerprint of a signing key. The signing key is the root key
                      |defining a so-called namespace, where the signing key has the ultimate control over
                      |issuing new identifiers.
                      |During initialisation, we have to pick such a unique identifier.
                      |By default, initialisation happens automatically, but it can be turned off by setting the auto-init
                      |option to false.
                      |Automatic node initialisation is usually turned off to preserve the identity of a participant or domain
                      |node (during major version upgrades) or if the topology transactions are managed through
                      |a different topology manager than the one integrated into this node.""")
  def init_id(identifier: Identifier, fingerprint: Fingerprint): UniqueIdentifier =
    consoleEnvironment.run {
      runner.adminCommand(TopologyAdminCommands.Init.InitId(identifier.unwrap, fingerprint.unwrap))
    }

}
