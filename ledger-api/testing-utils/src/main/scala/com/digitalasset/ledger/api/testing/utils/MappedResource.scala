// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

final case class MappedResource[Source, +Target](
    underlying: Resource[Source],
    transform: Source => Target,
) extends Resource[Target] {

  override def value: Target = transform(underlying.value)

  override def setup(): Unit = underlying.setup()

  override def close(): Unit = underlying.close()
}
