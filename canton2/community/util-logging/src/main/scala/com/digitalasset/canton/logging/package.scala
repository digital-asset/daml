// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.scalalogging.LoggerTakingImplicit

package object logging {

  /** Alias the complex logger type we use which expects a implicit TraceContext to something more memorable */
  type TracedLogger = LoggerTakingImplicit[TraceContext]

}
