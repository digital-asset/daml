// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.client.DelayedSequencerClient.{
  Immediate,
  SequencedEventDelayPolicy,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait DelaySequencedEvent {
  def delay(event: SequencedSerializedEvent): Future[Unit]
}

case object NoDelay extends DelaySequencedEvent {
  override def delay(event: SequencedSerializedEvent): Future[Unit] = Future.unit
}

final case class DelayedSequencerClient(synchronizerId: PhysicalSynchronizerId, member: String)
    extends DelaySequencedEvent {

  private[this] val onPublish: AtomicReference[SequencedEventDelayPolicy] =
    new AtomicReference[SequencedEventDelayPolicy](_ => Immediate)

  def setDelayPolicy(publishPolicy: SequencedEventDelayPolicy): Unit =
    onPublish.set(publishPolicy)

  override def delay(event: SequencedSerializedEvent): Future[Unit] = {
    val temp = onPublish.get()
    temp(event).until
  }
}

object DelayedSequencerClient {

  private val clients
      : concurrent.Map[(String, PhysicalSynchronizerId, String), DelayedSequencerClient] =
    new TrieMap[(String, PhysicalSynchronizerId, String), DelayedSequencerClient]

  def delayedSequencerClient(
      environmentId: String,
      synchronizerId: PhysicalSynchronizerId,
      member: String,
  ): Option[DelayedSequencerClient] =
    clients.get((environmentId, synchronizerId, member))

  def registerAndCreate(
      environmentId: String,
      synchronizerId: PhysicalSynchronizerId,
      member: String,
  ): DelayedSequencerClient = {
    val delayedLog = new DelayedSequencerClient(synchronizerId, member)
    clients.putIfAbsent((environmentId, synchronizerId, member), delayedLog).discard
    delayedLog
  }

  trait SequencedEventDelayPolicy extends (SequencedSerializedEvent => DelaySequencerClient)

  sealed trait DelaySequencerClient {
    val until: Future[Unit]
  }

  case object Immediate extends DelaySequencerClient {
    override val until: Future[Unit] = Future.unit
  }

  final case class DelayUntil(override val until: Future[Unit]) extends DelaySequencerClient

}
