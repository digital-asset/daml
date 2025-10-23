// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.Port
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference

trait ConfigDefaults[Defaults, Self] { self: Self =>

  /** Returns this configuration with defaults set for the given edition if necessary. */
  def withDefaults(defaults: Defaults, edition: CantonEdition): Self
}

/*
 The `sealed abstract` combined with the `@VisibleForTesting` annotation of the factory method
 ensures that this is used only in tests.
 */
sealed abstract class DefaultPorts {
  class DefaultPort(private val startPort: Int) {
    private val portRef = new AtomicReference[Port](Port.tryCreate(startPort))
    private val maxPort = Port.tryCreate(startPort + 990)

    /** Sets a automatically allocated default port if not already set. */
    def setDefaultPort(optPort: Option[Port]): Option[Port] =
      optPort.orElse(Some(portRef.getAndUpdate { port =>
        val next = port + portStep
        if (next > maxPort)
          sys.error(
            s"Default port assignment failed due to port $next being higher than upper bound $maxPort"
          )
        else next
      }))
  }

  private def defaultPortStart(portNo: Int): DefaultPort = new DefaultPort(portNo)

  /** Participant node default ports */
  val ledgerApiPort = defaultPortStart(4001)
  val participantAdminApiPort = defaultPortStart(4002)
  val jsonLedgerApiPort = defaultPortStart(4003)

  /** External sequencer node default ports (enterprise-only) */
  val sequencerPublicApiPort = defaultPortStart(5001)
  val sequencerAdminApiPort = defaultPortStart(5002)

  /** External mediator node default port (enterprise-only) */
  val mediatorAdminApiPort = defaultPortStart(6002)

  /** Increase the default port number for each new instance by portStep */
  private val portStep = 10
}

object DefaultPorts {
  // The `@VisibleForTesting` annotation combined with the `sealed abstract` ensures that this is used only in tests.
  @VisibleForTesting
  def create(): DefaultPorts = new DefaultPorts {}
}
