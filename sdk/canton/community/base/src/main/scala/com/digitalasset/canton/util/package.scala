// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.logging.NamedLoggingContext

package object util {
  type NamedLoggingLazyVal[T] = LazyValWithContext[T, NamedLoggingContext]
  val NamedLoggingLazyVal: LazyValWithContextCompanion[NamedLoggingContext] =
    new LazyValWithContextCompanion[NamedLoggingContext] {}
}
