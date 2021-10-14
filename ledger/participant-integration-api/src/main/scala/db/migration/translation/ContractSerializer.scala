// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.translation

import java.io.InputStream

import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.Value.VersionedContractInstance
import com.daml.lf.value.ValueCoder

private[migration] trait ContractSerializer {
  def serializeContractInstance(
      coinst: VersionedContractInstance
  ): Either[ValueCoder.EncodeError, Array[Byte]]

  def deserializeContractInstance(
      stream: InputStream
  ): Either[ValueCoder.DecodeError, VersionedContractInstance]
}

/** This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
private[migration] object ContractSerializer extends ContractSerializer {

  override def serializeContractInstance(
      coinst: VersionedContractInstance
  ): Either[ValueCoder.EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeContractInstance(ValueCoder.CidEncoder, coinst)
      .map(_.toByteArray())

  override def deserializeContractInstance(
      stream: InputStream
  ): Either[ValueCoder.DecodeError, VersionedContractInstance] =
    ValueSerializer.handleDeprecatedValueVersions(
      TransactionCoder
        .decodeVersionedContractInstance(
          ValueCoder.CidDecoder,
          TransactionOuterClass.ContractInstance.parseFrom(
            ValueSerializer.lfValueCodedInputStream(stream)
          ),
        )
    )

}
