// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import com.google.protobuf.DescriptorProtos.MethodDescriptorProto

final case class ServiceDescriptorInfo(
    fullServiceName: String,
    methods: Set[MethodDescriptorInfo],
)

object ServiceDescriptorInfo {

  def apply(
      packageName: String,
      serviceName: String,
      methods: Iterable[MethodDescriptorProto],
  ): ServiceDescriptorInfo = {
    val fullServiceName: String = s"$packageName.$serviceName"
    ServiceDescriptorInfo(
      fullServiceName = fullServiceName,
      methods = methods.view.map(MethodDescriptorInfo(fullServiceName, _)).toSet,
    )
  }

}
