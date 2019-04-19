// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation

import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.EventId

trait TransactionSerializer {

  def serialiseTransaction(transaction: GenTransaction.WithTxValue[EventId, AbsoluteContractId])
    : Either[EncodeError, Array[Byte]]

  def deserializeTransaction(blob: Array[Byte])
    : Either[DecodeError, GenTransaction.WithTxValue[EventId, AbsoluteContractId]]

}

object TransactionSerializer extends TransactionSerializer {

  private val defaultNidEncode: TransactionCoder.EncodeNid[EventId] = identity
  private val defaultDecodeNid: String => Either[DecodeError, EventId] = s => Right(s)

  override def serialiseTransaction(
      transaction: GenTransaction.WithTxValue[EventId, AbsoluteContractId])
    : Either[EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeTransactionWithCustomVersion(
        defaultNidEncode,
        ContractSerializer.defaultCidEncode,
        VersionedTransaction(
          TransactionVersions.assignVersion(transaction),
          transaction
        )
      )
      .map(_.toByteArray())

  override def deserializeTransaction(blob: Array[Byte])
    : Either[DecodeError, GenTransaction.WithTxValue[EventId, AbsoluteContractId]] =
    TransactionCoder
      .decodeVersionedTransaction(
        defaultDecodeNid,
        ContractSerializer.defaultCidDecode,
        TransactionOuterClass.Transaction.parseFrom(blob))
      .map(_.transaction)

}
