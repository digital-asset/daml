// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.translation

import java.io.InputStream

import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.daml.lf.value.ValueCoder

trait ContractSerializer {
  def serializeContractInstance(coinst: ContractInst[VersionedValue[AbsoluteContractId]])
    : Either[ValueCoder.EncodeError, Array[Byte]]

  def deserializeContractInstance(stream: InputStream)
    : Either[ValueCoder.DecodeError, ContractInst[VersionedValue[AbsoluteContractId]]]
}

/**
  * This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
object ContractSerializer extends ContractSerializer {

  override def serializeContractInstance(coinst: ContractInst[VersionedValue[AbsoluteContractId]])
    : Either[ValueCoder.EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeContractInstance[AbsoluteContractId](ValueCoder.CidEncoder, coinst)
      .map(_.toByteArray())

  override def deserializeContractInstance(stream: InputStream)
    : Either[ValueCoder.DecodeError, ContractInst[VersionedValue[AbsoluteContractId]]] =
    TransactionCoder
      .decodeContractInstance[AbsoluteContractId](
        ValueCoder.AbsCidDecoder,
        TransactionOuterClass.ContractInstance.parseFrom(
          Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT))
      )

}
