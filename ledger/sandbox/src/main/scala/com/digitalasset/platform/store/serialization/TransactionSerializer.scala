// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.serialization

import com.digitalasset.daml.lf.archive.{Decode, Reader}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.digitalasset.ledger.EventId

trait TransactionSerializer {

  def serializeTransaction(
      transaction: GenTransaction[EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]])
    : Either[EncodeError, Array[Byte]]

  def deserializeTransaction(blob: Array[Byte]): Either[
    DecodeError,
    GenTransaction[EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]]

}

object TransactionSerializer extends TransactionSerializer {

  override def serializeTransaction(
      transaction: GenTransaction[EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]])
    : Either[EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeTransaction(
        TransactionCoder.EventIdEncoder,
        ValueCoder.CidEncoder,
        transaction
      )
      .map(_.toByteArray())

  override def deserializeTransaction(blob: Array[Byte]): Either[
    DecodeError,
    GenTransaction[EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]] =
    TransactionCoder
      .decodeVersionedTransaction(
        TransactionCoder.EventIdDecoder,
        ValueCoder.AbsCidDecoder,
        TransactionOuterClass.Transaction.parseFrom(
          Decode.damlLfCodedInputStreamFromBytes(blob, Reader.PROTOBUF_RECURSION_LIMIT))
      )
      .map(_.transaction)

}
