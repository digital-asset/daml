// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggingContext}
import com.digitalasset.canton.tracing.TraceContext

package object util {
  type TracedLazyVal[T] = LazyValWithContext[T, TraceContext]
  val TracedLazyVal: LazyValWithContextCompanion[TraceContext] =
    new LazyValWithContextCompanion[TraceContext] {}

  type ErrorLoggingLazyVal[T] = LazyValWithContext[T, ErrorLoggingContext]
  val ErrorLoggingLazyVal: LazyValWithContextCompanion[ErrorLoggingContext] =
    new LazyValWithContextCompanion[ErrorLoggingContext] {}

  type NamedLoggingLazyVal[T] = LazyValWithContext[T, NamedLoggingContext]
  val NamedLoggingLazyVal: LazyValWithContextCompanion[NamedLoggingContext] =
    new LazyValWithContextCompanion[NamedLoggingContext] {}
}
