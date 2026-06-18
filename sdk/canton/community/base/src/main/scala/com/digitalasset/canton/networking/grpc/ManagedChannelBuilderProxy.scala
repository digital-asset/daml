// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

/** Proxy for a [[io.grpc.ManagedChannelBuilder]] with unknown type argument type
  *
  * Hides the wildcard type argument to `ManagedChannelBuilder` so that type inference will not
  * infer the more correct argument `A forSome { type A <: ManagedChannelBuilder[A] }`, which is an
  * existential type that cannot be expressed by wildcards. This works only because Scala does not
  * check the bounds of wildcard type arguments.
  *
  * See https://stackoverflow.com/a/5520212 for some background on existential types
  */
class ManagedChannelBuilderProxy(private val builder: ManagedChannelBuilder[?]) extends AnyVal {
  def build(): ManagedChannel = builder.build()
}

object ManagedChannelBuilderProxy {
  def apply(builder: ManagedChannelBuilder[?]): ManagedChannelBuilderProxy =
    new ManagedChannelBuilderProxy(builder)
}
