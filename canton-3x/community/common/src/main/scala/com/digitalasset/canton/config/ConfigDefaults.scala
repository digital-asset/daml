// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.Port

import java.util.concurrent.atomic.AtomicReference

trait ConfigDefaults[Defaults, Self] { self: Self =>

  /** Returns this configuration with defaults set if necessary. */
  def withDefaults(defaults: Defaults): Self
}

class DefaultPorts {

  class DefaultPort(private val startPort: Int) {
    private val portRef = new AtomicReference[Port](Port.tryCreate(startPort))
    private val maxPort = Port.tryCreate(startPort + 190)

    /** Sets a automatically allocated default port if not already set. */
    def setDefaultPort[C](optPort: Option[Port]): Option[Port] =
      optPort.orElse(Some(portRef.getAndUpdate { port =>
        val next = port + portStep
        if (next > maxPort)
          sys.error(
            s"Default port assignment failed due to port $next being higher than upper bound $maxPort"
          )
        else next
      }))

    def reset(): Unit = portRef.set(Port.tryCreate(startPort))
  }

  private def defaultPortStart(portNo: Int): DefaultPort = new DefaultPort(portNo)

  // user-manual-entry-begin: ConfigDefaults
  /** Participant node default ports */
  val ledgerApiPort = defaultPortStart(4001)
  val participantAdminApiPort = defaultPortStart(4002)

  /** Domain node default ports */
  val domainPublicApiPort = defaultPortStart(4201)
  val domainAdminApiPort = defaultPortStart(4202)

  /** External sequencer node default ports (enterprise-only) */
  val sequencerPublicApiPort = defaultPortStart(4401)
  val sequencerAdminApiPort = defaultPortStart(4402)

  /** External mediator node default port (enterprise-only) */
  val mediatorAdminApiPort = defaultPortStart(4602)

  /** Domain node default ports */
  val domainManagerAdminApiPort = defaultPortStart(4801)

  /** External sequencer node x default ports (enterprise-only) */
  val sequencerXPublicApiPort = defaultPortStart(5001)
  val sequencerXAdminApiPort = defaultPortStart(5002)

  /** External mediator node x default port (enterprise-only) */
  val mediatorXAdminApiPort = defaultPortStart(5202)

  /** Increase the default port number for each new instance by portStep */
  private val portStep = 10
  // user-manual-entry-end: ConfigDefaults

}
