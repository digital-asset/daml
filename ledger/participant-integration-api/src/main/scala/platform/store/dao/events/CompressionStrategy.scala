// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.platform.store.serialization.Compression

final case class CompressionStrategy(
    createArgumentCompression: Compression.Algorithm,
    createKeyValueCompression: Compression.Algorithm,
    exerciseArgumentCompression: Compression.Algorithm,
    exerciseResultCompression: Compression.Algorithm,
)

object CompressionStrategy {

  val None: CompressionStrategy = CompressionStrategy(
    createArgumentCompression = Compression.Algorithm.None,
    createKeyValueCompression = Compression.Algorithm.None,
    exerciseArgumentCompression = Compression.Algorithm.None,
    exerciseResultCompression = Compression.Algorithm.None,
  )

  val AllGZIP: CompressionStrategy = CompressionStrategy(
    createArgumentCompression = Compression.Algorithm.GZIP,
    createKeyValueCompression = Compression.Algorithm.GZIP,
    exerciseArgumentCompression = Compression.Algorithm.GZIP,
    exerciseResultCompression = Compression.Algorithm.GZIP,
  )
}
