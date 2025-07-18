// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]] when looking for
  * discarded futures.
  */
final class DoNotDiscardLikeFuture extends StaticAnnotation
