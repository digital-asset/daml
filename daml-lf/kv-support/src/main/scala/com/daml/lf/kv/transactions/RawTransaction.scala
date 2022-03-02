// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.google.protobuf.ByteString

/** Stores [[com.daml.lf.transaction.TransactionOuterClass.Transaction]] as a [[ByteString]]. */
case class RawTransaction(byteString: ByteString) extends AnyVal

object RawTransaction {

  /** Stores [[com.daml.lf.transaction.TransactionOuterClass.Node]] as a [[ByteString]]. */
  case class Node(byteString: ByteString) extends AnyVal

  /** We store node IDs as strings (see [[com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionBlindingInfo.DisclosureEntry]]). */
  case class NodeId(value: String) extends AnyVal
}
