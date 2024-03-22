// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.google.protobuf.{AbstractMessageLite, ByteString, CodedOutputStream}

object SafeProto {

  // `AbstractMessageLite#toByteString` and `AbstractMessageLite#toByteArray` on
  // messages requiring more than 2GB fails with two different RuntimeExceptions
  // which are not human readable. The root cause of the failure is that
  // AbstractMessageLite#getSerializedSize computation does not handle overflow.
  // We identified two different error cases when `getSerializedSize` overflows:
  //  - if `getSerializedSize` overflows and outputs a positive int,
  //    the serialization throws a generic `RuntimeException` with a
  //    `CodedOutputStream.OutOfSpaceException` as cause.
  //  - if `getSerializedSize` overflows and outputs a negative int,
  //    the serialization throws a `NegativeArraySizeException`
  // This function catches those exceptions.
  private[this] def safelySerialize[X](
      message: AbstractMessageLite[_, _],
      serialize: AbstractMessageLite[_, _] => X,
  ): Either[String, X] =
    try {
      Right(serialize(message))
    } catch {
      case e: RuntimeException
          if e.isInstanceOf[NegativeArraySizeException] ||
            e.getCause != null && e.getCause.isInstanceOf[CodedOutputStream.OutOfSpaceException] =>
        Left(s"the ${message.getClass.getName} message is too large to be serialized")
    }

  def toByteString(message: AbstractMessageLite[_, _]): Either[String, ByteString] =
    safelySerialize(message, _.toByteString)

  def toByteArray(message: AbstractMessageLite[_, _]): Either[String, Array[Byte]] =
    safelySerialize(message, _.toByteArray)
}
