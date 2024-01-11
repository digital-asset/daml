// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, v1, v2, v3, v4}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  RepresentativeProtocolVersion,
}

/** Parent trait of messages that are sent through the sequencer
  */
trait ProtocolMessage
    extends Product
    with Serializable
    with HasDomainId
    with PrettyPrinting
    with HasRepresentativeProtocolVersion {

  override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type]

  /** The ID of the domain over which this message is supposed to be sent. */
  def domainId: DomainId

  /** By default prints only the object name as a trade-off for shorter long lines and not leaking confidential data.
    * Sub-classes may override the pretty instance to print more information.
    */
  override def pretty: Pretty[this.type] = prettyOfObject[ProtocolMessage]
}

/** Marker trait for [[ProtocolMessage]]s that are not a [[SignedProtocolMessage]] */
trait UnsignedProtocolMessage extends ProtocolMessage

/** Trait for [[ProtocolMessage]]s that can be serialized as a v0 [[EnvelopeContent]] */
trait ProtocolMessageV0 extends ProtocolMessage {
  protected[messages] def toProtoEnvelopeContentV0: v0.EnvelopeContent
}

/** Trait for [[ProtocolMessage]]s that can be serialized as a v1 [[EnvelopeContent]] */
trait ProtocolMessageV1 extends ProtocolMessage {
  protected[messages] def toProtoEnvelopeContentV1: v1.EnvelopeContent
}

/** Trait for [[ProtocolMessage]]s that can be serialized as a v2 [[EnvelopeContent]] */
trait ProtocolMessageV2 extends ProtocolMessage {
  protected[messages] def toProtoEnvelopeContentV2: v2.EnvelopeContent
}

/** Trait for [[ProtocolMessage]]s that can be serialized as a v3 [[EnvelopeContent]] */
trait ProtocolMessageV3 extends ProtocolMessage {
  protected[messages] def toProtoEnvelopeContentV3: v3.EnvelopeContent
}

/** Trait for [[ProtocolMessage]]s that can be serialized as a v4 [[EnvelopeContent]] */
trait UnsignedProtocolMessageV4 extends ProtocolMessage {
  protected[messages] def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent
}

object ProtocolMessage {

  /** Returns the envelopes from the batch that match the given domain ID. If any other messages exist, it gives them
    * to the provided callback
    */
  def filterDomainsEnvelopes[M <: ProtocolMessage](
      batch: Batch[OpenEnvelope[M]],
      domainId: DomainId,
      onWrongDomain: List[OpenEnvelope[M]] => Unit,
  ): List[OpenEnvelope[M]] = {
    val (withCorrectDomainId, withWrongDomainId) =
      batch.envelopes.partition(_.protocolMessage.domainId == domainId)
    if (withWrongDomainId.nonEmpty)
      onWrongDomain(withWrongDomainId)
    withCorrectDomainId
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
