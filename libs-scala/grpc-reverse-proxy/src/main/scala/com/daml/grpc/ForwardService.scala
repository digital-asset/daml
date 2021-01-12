// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import java.io.InputStream

import com.daml.grpc.reflection.ServiceDescriptorInfo
import com.google.protobuf.ByteString
import io.grpc.{CallOptions, Channel, MethodDescriptor, ServerServiceDefinition}

private[grpc] object ForwardService {

  def apply(backend: Channel, service: ServiceDescriptorInfo): ServerServiceDefinition =
    service.methods
      .map(_.toMethodDescriptor(NoopMarshaller, NoopMarshaller))
      .map(ForwardCall(_, backend, CallOptions.DEFAULT))
      .foldLeft(ServerServiceDefinition.builder(service.fullServiceName))(_ addMethod _)
      .build()

  private object NoopMarshaller extends MethodDescriptor.Marshaller[ByteString] {
    override def parse(input: InputStream): ByteString = ByteString.readFrom(input)
    override def stream(bytes: ByteString): InputStream = bytes.newInput()
  }

}
