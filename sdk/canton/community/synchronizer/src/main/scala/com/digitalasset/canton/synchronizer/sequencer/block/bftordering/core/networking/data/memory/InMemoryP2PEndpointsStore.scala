// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.memory

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.blocking
import scala.util.{Success, Try}

abstract class GenericInMemoryP2PEndpointsStore[E <: Env[E]](
    initialEndpoints: Set[P2PEndpoint]
) extends P2PEndpointsStore[E] {

  private val endpoints = new mutable.HashMap[P2PEndpoint.Id, P2PEndpoint]
  initialEndpoints.foreach(endpoint => endpoints.put(endpoint.id, endpoint).discard)

  protected def createFuture[A](action: String)(x: () => Try[A]): E#FutureUnlessShutdownT[A]

  override final def listEndpoints(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[P2PEndpoint]] =
    blocking {
      synchronized {
        createFuture("") { () =>
          Success(
            endpoints.keySet.toSeq.sorted.map(endpointId => endpoints(endpointId))
          )
        }
      }
    }

  override final def addEndpoint(endpoint: P2PEndpoint)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Boolean] =
    blocking {
      synchronized {
        createFuture("") { () =>
          val endpointId = endpoint.id
          val changed =
            if (!endpoints.contains(endpointId)) {
              endpoints.addOne(endpointId -> endpoint).discard
              true
            } else {
              false
            }
          Success(changed)
        }
      }
    }

  override final def removeEndpoint(endpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Boolean] =
    blocking {
      synchronized {
        createFuture("") { () =>
          Success(endpoints.remove(endpointId).isDefined)
        }
      }
    }

  override final def clearAllEndpoints()(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit] =
    blocking {
      synchronized {
        createFuture("") { () =>
          Success {
            endpoints.clear()
          }
        }
      }
    }
}

final class InMemoryP2PEndpointsStore(
    initialEndpoints: Set[P2PEndpoint] = Set.empty
) extends GenericInMemoryP2PEndpointsStore[PekkoEnv](initialEndpoints) {

  override def createFuture[A](action: String)(x: () => Try[A]): PekkoFutureUnlessShutdown[A] =
    PekkoFutureUnlessShutdown(action, () => FutureUnlessShutdown.fromTry(x()))

  override def close(): Unit = ()
}
