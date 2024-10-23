// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext

object SimulationClient {

  /** A simulation client initializer defines how a simulation client module is initialized.
    */
  trait Initializer[E <: Env[E], ClientMessageT, SystemInputMessageT] {

    /** Create the client that is represented as a Module
      * @param systemRef is used to send messages to the system under test
      */
    def createClient(
        systemRef: ModuleRef[SystemInputMessageT]
    ): Module[E, ClientMessageT]

    /** This will be called once after the client has been created. This can be used to schedule
      *  the initial messages to the client
      * @param context for the client
      */
    def init(context: E#ActorContextT[ClientMessageT]): Unit
  }
}

final case class EmptyClient[E <: Env[E], ClientMessageT](
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, ClientMessageT] {

  override def receiveInternal(
      message: ClientMessageT
  )(implicit context: E#ActorContextT[ClientMessageT], traceContext: TraceContext): Unit = {}
}

object EmptyClient {

  def initializer[ClientMessageT, SystemInputMessageT](
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SimulationClient.Initializer[SimulationEnv, ClientMessageT, SystemInputMessageT] =
    new SimulationClient.Initializer[SimulationEnv, ClientMessageT, SystemInputMessageT] {
      override def createClient(
          systemRef: ModuleRef[SystemInputMessageT]
      ): Module[SimulationEnv, ClientMessageT] = EmptyClient(loggerFactory, timeouts)

      override def init(
          context: SimulationModuleSystem.SimulationModuleContext[ClientMessageT]
      ): Unit = {}
    }
}
