// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source

object PerfSupport {

  // adds a buffer to the output of the original source, and adds a Counter metric for buffer size
  // good for detecting consumer vs producer speed
  def instrumentedBufferedSource[T, U](
      original: Source[T, U],
      counter: com.codahale.metrics.Counter,
      size: Int,
  ): Source[T, U] = {
    original
      .wireTap(_ => counter.inc())
      .buffer(size, OverflowStrategy.backpressure)
      .wireTap(_ => counter.dec())
  }

}
