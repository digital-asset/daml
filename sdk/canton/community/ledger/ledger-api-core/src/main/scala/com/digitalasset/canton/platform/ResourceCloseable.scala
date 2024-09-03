// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.Resource
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.blocking

/** This helper class serves as a bridge between FlagCloseable (canton's shutdown/resource manager trait) and
  * Resource (daml's shutdown/resource management container).
  * Recommended usage:
  *   1 - The service class needs to be prepared with subclassing the ResourceCloseable.
  *   2 - As the service instance is created, the ResourceOwnerFlagCloseableOps should be used to
  *       instantiate a FlagCloseable with the ResourceOwner[ServiceClass].acquireFlagCloseable.
  *   3 - The resulting ServiceClass instance can be used as FlagCloseable: as it is getting closed,
  *       the wrapped Resource will be released.
  */
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
