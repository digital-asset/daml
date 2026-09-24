// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.ReassignmentId

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}

final class ReassignmentSynchronizer(
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends NamedLogging
    with FlagCloseable {

  /** unassignments that started phase 3, but have not yet completed phase 7 * */
  private val pendingUnassignments: TrieMap[ReassignmentId, Promise[Unit]] =
    TrieMap.empty[ReassignmentId, Promise[Unit]]

  def add(reassignmentId: ReassignmentId): Unit =
    pendingUnassignments.putIfAbsent(reassignmentId, Promise()).discard

  def completePhase7(reassignmentId: ReassignmentId): Unit =
    pendingUnassignments.remove(reassignmentId).foreach(_.trySuccess(()))

  def find(reassignmentId: ReassignmentId): Option[Future[Unit]] =
    pendingUnassignments.get(reassignmentId).map(_.future)

  override def onClosed(): Unit =
    pendingUnassignments.foreach { case (_, promise) =>
      promise.trySuccess(()).discard
    }

}
