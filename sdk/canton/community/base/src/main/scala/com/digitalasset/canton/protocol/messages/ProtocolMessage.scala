// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

/** Parent trait of messages that are sent through the sequencer
  */
trait ProtocolMessage
    extends Product
    with Serializable
    with HasSynchronizerId
    with PrettyPrinting
    with HasRepresentativeProtocolVersion {

  override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type]

  /** The ID of the synchronizer over which this message is supposed to be sent. */
  def synchronizerId: SynchronizerId

  /** By default prints only the object name as a trade-off for shorter long lines and not leaking confidential data.
    * Sub-classes may override the pretty instance to print more information.
    */
  @VisibleForTesting
  override def pretty: Pretty[this.type] = prettyOfObject[ProtocolMessage]
}

/** Marker trait for [[ProtocolMessage]]s that are not a [[SignedProtocolMessage]] */
trait UnsignedProtocolMessage extends ProtocolMessage {
  protected[messages] def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent
}

object ProtocolMessage {

  /** Returns the envelopes from the batch that match the given synchronizer id. If any other messages exist, it gives them
    * to the provided callback
    */
  def filterSynchronizerEnvelopes[M <: ProtocolMessage](
      batch: Batch[OpenEnvelope[M]],
      synchronizerId: SynchronizerId,
      onWrongSynchronizer: List[OpenEnvelope[M]] => Unit,
  ): List[OpenEnvelope[M]] = {
    val (withCorrectSynchronizerId, withWrongSynchronizerId) =
      batch.envelopes.partition(_.protocolMessage.synchronizerId == synchronizerId)
    if (withWrongSynchronizerId.nonEmpty)
      onWrongSynchronizer(withWrongSynchronizerId)
    withCorrectSynchronizerId
  }

  trait ProtocolMessageContentCast[A <: ProtocolMessage] {
    def toKind(message: ProtocolMessage): Option[A]
    def targetKind: String
  }

  object ProtocolMessageContentCast {
    def create[A <: ProtocolMessage](name: String)(
        cast: ProtocolMessage => Option[A]
    ): ProtocolMessageContentCast[A] = new ProtocolMessageContentCast[A] {
      override def toKind(message: ProtocolMessage): Option[A] = cast(message)

      override def targetKind: String = name
    }
  }

  def toKind[M <: ProtocolMessage](envelope: DefaultOpenEnvelope)(implicit
      cast: ProtocolMessageContentCast[M]
  ): Option[M] =
    cast.toKind(envelope.protocolMessage)

  def select[M <: ProtocolMessage](envelope: DefaultOpenEnvelope)(implicit
      cast: ProtocolMessageContentCast[M]
  ): Option[OpenEnvelope[M]] =
    envelope.traverse(cast.toKind)
}
