// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait PruningOffsetService {
  def pruningOffset(implicit
      traceContext: TraceContext
  ): Future[Option[Offset]]
}
