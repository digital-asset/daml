// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]] when looking for
  * traverse-like calls with such an applicative instance.
  */
final class DoNotTraverseLikeFuture extends StaticAnnotation
