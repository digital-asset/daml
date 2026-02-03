// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml

import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.{AbstractMessageLite, ByteString, CodedOutputStream, Message}

import scala.jdk.CollectionConverters._

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
      message: AbstractMessageLite[?, ?],
      serialize: AbstractMessageLite[?, ?] => X,
  ): Either[String, X] =
    try {
      Right(serialize(message))
    } catch {
      case e: RuntimeException
          if e.isInstanceOf[NegativeArraySizeException] ||
            e.getCause != null && e.getCause.isInstanceOf[CodedOutputStream.OutOfSpaceException] =>
        Left(s"the ${message.getClass.getName} message is too large to be serialized")
    }

  def toByteString(message: AbstractMessageLite[?, ?]): Either[String, ByteString] =
    safelySerialize(message, _.toByteString)

  def toByteArray(message: AbstractMessageLite[?, ?]): Either[String, Array[Byte]] =
    safelySerialize(message, _.toByteArray)

  def ensureNoUnknownFields(msg: Message): Either[String, Unit] =
    msg.getUnknownFields.asMap().keySet().asScala.headOption match {
      case Some(n) =>
        Left(
          s"Message of type ${msg.getDescriptorForType.getFullName} contains unknown field numbered $n"
        )
      case None =>
        msg.getAllFields.asScala.foldLeft[Either[String, Unit]](Right(())) {
          case (acc @ Left(_), _) => acc
          case (acc, (descriptor, field)) =>
            descriptor.getType match {
              case FieldDescriptor.Type.MESSAGE =>
                if (descriptor.isRepeated)
                  field
                    .asInstanceOf[java.util.List[Message]]
                    .asScala
                    .foldLeft[Either[String, Unit]](Right(())) {
                      case (acc @ Left(_), _) => acc
                      case (_, msg) => ensureNoUnknownFields(msg)
                    }
                else
                  ensureNoUnknownFields(field.asInstanceOf[Message])
              case FieldDescriptor.Type.GROUP =>
                Left("Groups are not supported")
              case _ =>
                acc
            }
        }
    }

}
