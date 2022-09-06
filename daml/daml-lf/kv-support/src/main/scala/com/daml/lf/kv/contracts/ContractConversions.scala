// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.contracts

import com.daml.SafeProto
import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.{Value, ValueCoder}

import scala.util.{Failure, Success, Try}

object ContractConversions {

  def encodeContractInstance(
      coinst: Value.VersionedContractInstance
  ): Either[ValueCoder.EncodeError, RawContractInstance] =
    for {
      message <- TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, coinst)
      bytes <- SafeProto.toByteString(message).left.map(ValueCoder.EncodeError(_))
    } yield RawContractInstance(bytes)

  def decodeContractInstance(
      rawContractInstance: RawContractInstance
  ): Either[ConversionError, Value.VersionedContractInstance] =
    Try(TransactionOuterClass.ContractInstance.parseFrom(rawContractInstance.byteString)) match {
      case Success(contractInstance) =>
        TransactionCoder
          .decodeVersionedContractInstance(ValueCoder.CidDecoder, contractInstance)
          .left
          .map(ConversionError.DecodeError)
      case Failure(throwable) => Left(ConversionError.ParseError(throwable.getMessage))
    }
}
