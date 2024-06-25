// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.Resource
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.blocking

@SuppressWarnings(Array("org.wartremover.warts.Var"))
abstract class ResourceCloseable extends FlagCloseableAsync with NamedLogging {
  private var closeableResource: Option[AsyncCloseable] = None

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = blocking(synchronized {
    List(
      closeableResource.getOrElse(
        throw new IllegalStateException(
          "Programming error: resource not registered. Please use ResourceOwnerOps.toCloseable."
        )
      )
    )
  })

  def registerResource(resource: Resource[?], name: String)(implicit
      traceContext: TraceContext
  ): this.type = blocking(synchronized {
    this.closeableResource.foreach(_ =>
      throw new IllegalStateException(
        "Programming error: resource registered multiple times. Please use ResourceOwnerFlagCloseableOps.acquireFlagCloseable."
      )
    )
    this.closeableResource = Some(
      AsyncCloseable(
        name = name,
        closeFuture = resource.release(),
        timeout = timeouts.shutdownNetwork,
        onTimeout = err =>
          logger.warn(s"Resource $name failed to close within ${timeouts.shutdownNetwork}.", err),
      )
    )
    this
  })
}
