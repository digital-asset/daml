// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import java.io.{ByteArrayInputStream, InputStream}

import com.daml.grpc.reflection.ServiceDescriptorInfo
import com.google.common.io.ByteStreams
import io.grpc.{CallOptions, Channel, MethodDescriptor, ServerServiceDefinition}

private[grpc] object ForwardService {

  def apply(backend: Channel, service: ServiceDescriptorInfo): ServerServiceDefinition =
    service.methods
      .map(_.toMethodDescriptor(ByteArrayMarshaller, ByteArrayMarshaller))
      .map(ForwardCall(_, backend, CallOptions.DEFAULT))
      .foldLeft(ServerServiceDefinition.builder(service.fullServiceName))(_ addMethod _)
      .build()

  private object ByteArrayMarshaller extends MethodDescriptor.Marshaller[Array[Byte]] {
    override def parse(input: InputStream): Array[Byte] = ByteStreams.toByteArray(input)
    override def stream(bytes: Array[Byte]): InputStream = new ByteArrayInputStream(bytes)
  }

}
