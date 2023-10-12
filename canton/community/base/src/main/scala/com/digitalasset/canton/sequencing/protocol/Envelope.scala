// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.topology.Member

/** An [[Envelope]] wraps an envelope content such as a [[com.digitalasset.canton.protocol.messages.ProtocolMessage]]
  * together with the recipients.
  *
  * @tparam M The type of the envelope content
  */
trait Envelope[+M] extends PrettyPrinting {

  def recipients: Recipients

  def forRecipient(
      member: Member,
      groupAddresses: Set[GroupRecipient],
  ): Option[Envelope[M]]

  /** Closes the envelope by serializing the contents if necessary */
  def closeEnvelope: ClosedEnvelope
}
