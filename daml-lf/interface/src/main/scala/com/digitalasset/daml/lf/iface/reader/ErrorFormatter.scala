// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface.reader

import com.digitalasset.daml.lf.iface.reader.InterfaceReader.{
  InvalidDataTypeDefinition,
  UnserializableDataType
}
import com.google.protobuf.Descriptors.GenericDescriptor
import com.google.protobuf.{MessageOrBuilder, ProtocolMessageEnum}

object ErrorFormatter {

  sealed trait HasDescriptor[-A] {
    def descriptor(a: A): GenericDescriptor
  }

  implicit object messageOrBuilderDescriptor extends HasDescriptor[MessageOrBuilder] {
    override def descriptor(a: MessageOrBuilder): GenericDescriptor = a.getDescriptorForType
  }

  implicit object protocolMessageEnumDescriptor extends HasDescriptor[ProtocolMessageEnum] {
    override def descriptor(a: ProtocolMessageEnum): GenericDescriptor = a.getDescriptorForType
  }

  private def errorMessage[T: HasDescriptor](a: T, reason: String): String = {
    val evidence = implicitly[HasDescriptor[T]]
    val descriptor: GenericDescriptor = evidence.descriptor(a)
    val name: String = descriptor.getFullName
    s"Invalid protobuf message definition: ${a.getClass.getCanonicalName}, reason: $reason, $name: $a"
  }

  def invalidDataTypeDefinition[T: HasDescriptor](a: T, reason: String): InvalidDataTypeDefinition =
    InvalidDataTypeDefinition(errorMessage(a, reason))

  def unserializableDataType[T: HasDescriptor](a: T, reason: String): UnserializableDataType =
    UnserializableDataType(errorMessage(a, reason))
}
