// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.translation

import java.io.InputStream

import com.daml.ledger.EventId
import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.transaction._
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.ValueCoder
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}

trait TransactionSerializer {

  def serializeTransaction(
      transaction: GenTransaction[EventId, ContractId, VersionedValue[ContractId]])
    : Either[EncodeError, Array[Byte]]

  def deserializeTransaction(stream: InputStream)
    : Either[DecodeError, GenTransaction[EventId, ContractId, VersionedValue[ContractId]]]

}

object TransactionSerializer extends TransactionSerializer {

  override def serializeTransaction(
      transaction: GenTransaction[EventId, ContractId, VersionedValue[ContractId]])
    : Either[EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeTransaction(
        TransactionCoder.EventIdEncoder,
        ValueCoder.CidEncoder,
        transaction
      )
      .map(_.toByteArray())

  override def deserializeTransaction(stream: InputStream)
    : Either[DecodeError, GenTransaction[EventId, ContractId, VersionedValue[ContractId]]] =
    TransactionCoder
      .decodeVersionedTransaction(
        TransactionCoder.EventIdDecoder,
        ValueCoder.CidDecoder,
        TransactionOuterClass.Transaction.parseFrom(
          Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT))
      )
      .map(_.transaction)

}
