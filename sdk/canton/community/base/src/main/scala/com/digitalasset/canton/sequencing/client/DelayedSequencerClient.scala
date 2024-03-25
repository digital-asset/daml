// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.DelayedSequencerClient.{
  Immediate,
  SequencedEventDelayPolicy,
}
import com.digitalasset.canton.topology.DomainId

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait DelaySequencedEvent {
  def delay(event: OrdinarySerializedEvent): Future[Unit]
}

case object NoDelay extends DelaySequencedEvent {
  override def delay(event: OrdinarySerializedEvent): Future[Unit] = Future.unit
}

final case class DelayedSequencerClient(domain: DomainId, member: String)
    extends DelaySequencedEvent {

  private[this] val onPublish: AtomicReference[SequencedEventDelayPolicy] =
    new AtomicReference[SequencedEventDelayPolicy](_ => Immediate)

  def setDelayPolicy(publishPolicy: SequencedEventDelayPolicy): Unit = {
    onPublish.set(publishPolicy)
  }

  override def delay(event: OrdinarySerializedEvent): Future[Unit] = {
    val temp = onPublish.get()
    temp(event).until
  }
}

object DelayedSequencerClient {

  private val clients: concurrent.Map[(String, DomainId, String), DelayedSequencerClient] =
    new TrieMap[(String, DomainId, String), DelayedSequencerClient]

  def delayedSequencerClient(
      environmentId: String,
      domainId: DomainId,
      member: String,
  ): Option[DelayedSequencerClient] = {
    clients.get((environmentId, domainId, member))
  }

  def registerAndCreate(
      environmentId: String,
      domainId: DomainId,
      member: String,
  ): DelayedSequencerClient = {
    val delayedLog = new DelayedSequencerClient(domainId, member)
    clients.putIfAbsent((environmentId, domainId, member), delayedLog).discard
    delayedLog
  }

  trait SequencedEventDelayPolicy extends (OrdinarySerializedEvent => DelaySequencerClient)

  sealed trait DelaySequencerClient {
    val until: Future[Unit]
  }

  case object Immediate extends DelaySequencerClient {
    override val until: Future[Unit] = Future.unit
  }

  final case class DelayUntil(override val until: Future[Unit]) extends DelaySequencerClient

}
