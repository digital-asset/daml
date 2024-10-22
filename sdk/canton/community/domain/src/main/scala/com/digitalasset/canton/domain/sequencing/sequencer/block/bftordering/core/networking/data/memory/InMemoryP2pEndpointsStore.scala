// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.memory

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.P2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.blocking
import scala.util.{Success, Try}

abstract class GenericInMemoryP2pEndpointsStore[E <: Env[E]](initialEndpoints: Set[Endpoint])
    extends P2pEndpointsStore[E] {

  private val endpointsSet = new mutable.HashSet[Endpoint]
  endpointsSet ++= initialEndpoints

  protected def createFuture[A](action: String)(x: () => Try[A]): E#FutureUnlessShutdownT[A]

  override final def listEndpoints(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[Endpoint]] =
    blocking {
      synchronized {
        createFuture("") { () =>
          Success(
            endpointsSet.toSeq.sorted
          )
        }
      }
    }

  override final def addEndpoint(
      endpoint: Endpoint
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Boolean] =
    blocking {
      synchronized {
        createFuture("") { () =>
          val changed =
            if (!endpointsSet.contains(endpoint)) {
              val _ = endpointsSet += endpoint
              true
            } else {
              false
            }
          Success(changed)
        }
      }
    }

  override final def removeEndpoint(
      endpoint: Endpoint
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Boolean] =
    blocking {
      synchronized {
        createFuture("") { () =>
          Success(if (endpointsSet.contains(endpoint)) {
            val _ = endpointsSet -= endpoint
            true
          } else {
            false
          })
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
            endpointsSet.clear()
          }
        }
      }
    }
}

final class InMemoryP2pEndpointsStore(initialEndpoints: Set[Endpoint] = Set.empty)
    extends GenericInMemoryP2pEndpointsStore[PekkoEnv](initialEndpoints) {
  override def createFuture[A](action: String)(x: () => Try[A]): PekkoFutureUnlessShutdown[A] =
    PekkoFutureUnlessShutdown(action, FutureUnlessShutdown.fromTry(x()))
}

final class SimulationP2pEndpointsStore(initialEndpoints: Set[Endpoint] = Set.empty)
    extends GenericInMemoryP2pEndpointsStore[SimulationEnv](initialEndpoints) {
  override def createFuture[A](action: String)(x: () => Try[A]): SimulationFuture[A] =
    SimulationFuture(x)
}
