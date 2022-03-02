// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import com.google.protobuf.DescriptorProtos.MethodDescriptorProto
import io.grpc.MethodDescriptor
import io.grpc.MethodDescriptor.{MethodType, generateFullMethodName}

final case class MethodDescriptorInfo(
    fullMethodName: String,
    methodType: MethodType,
) {

  def toMethodDescriptor[ReqT, RespT](
      requestMarshaller: MethodDescriptor.Marshaller[ReqT],
      responseMarshaller: MethodDescriptor.Marshaller[RespT],
  ): MethodDescriptor[ReqT, RespT] =
    MethodDescriptor
      .newBuilder(requestMarshaller, responseMarshaller)
      .setType(methodType)
      .setFullMethodName(fullMethodName)
      .build()

}

object MethodDescriptorInfo {

  def apply(fullServiceName: String, method: MethodDescriptorProto): MethodDescriptorInfo =
    MethodDescriptorInfo(
      methodName = generateFullMethodName(fullServiceName, method.getName),
      clientStreaming = method.getClientStreaming,
      serverStreaming = method.getServerStreaming,
    )

  def apply(method: MethodDescriptor[_, _]): MethodDescriptorInfo =
    MethodDescriptorInfo(
      fullMethodName = method.getFullMethodName,
      methodType = method.getType,
    )

  def apply(
      methodName: String,
      clientStreaming: Boolean,
      serverStreaming: Boolean,
  ): MethodDescriptorInfo =
    MethodDescriptorInfo(
      fullMethodName = methodName,
      methodType = methodType(clientStreaming, serverStreaming),
    )

  private def methodType(clientStreaming: Boolean, serverStreaming: Boolean): MethodType =
    if (clientStreaming && serverStreaming) {
      MethodType.BIDI_STREAMING
    } else if (clientStreaming) {
      MethodType.CLIENT_STREAMING
    } else if (serverStreaming) {
      MethodType.SERVER_STREAMING
    } else {
      MethodType.UNARY
    }

}
