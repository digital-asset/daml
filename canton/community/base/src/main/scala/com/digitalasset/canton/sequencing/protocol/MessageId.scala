// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String73,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

import java.util.UUID

/** Identifier assigned by caller to a submission request. */
final case class MessageId(override protected val str: String73)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  override def pretty: Pretty[MessageId] = prettyOfString(_.unwrap)
}

object MessageId extends LengthLimitedStringWrapperCompanion[String73, MessageId] {
  def fromUuid(uuid: UUID): MessageId = MessageId(checked(String73.tryCreate(uuid.toString)))
  def randomMessageId(): MessageId = fromUuid(UUID.randomUUID())

  override protected def companion: String73.type = String73
  override def instanceName: String = "MessageId"
  override protected def factoryMethodWrapper(str: String73): MessageId = new MessageId(str)
}
