// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.contracts

import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.{Value, ValueCoder}

object ContractConversions {

  def encodeContractInstance(
      coinst: Value.VersionedContractInstance
  ): Either[ValueCoder.EncodeError, RawContractInstance] =
    TransactionCoder
      .encodeContractInstance(ValueCoder.CidEncoder, coinst)
      .map(contractInstance => RawContractInstance(contractInstance.toByteString))

  def decodeContractInstance(
      rawContractInstance: RawContractInstance
  ): Either[ValueCoder.DecodeError, Value.VersionedContractInstance] = {
    val contractInstance =
      TransactionOuterClass.ContractInstance.parseFrom(rawContractInstance.byteString)
    TransactionCoder.decodeVersionedContractInstance(ValueCoder.CidDecoder, contractInstance)
  }
}
