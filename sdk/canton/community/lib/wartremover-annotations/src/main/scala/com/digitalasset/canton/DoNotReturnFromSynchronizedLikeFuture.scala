// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]] when looking at
  * the return types of synchronized blocks.
  */
final class DoNotReturnFromSynchronizedLikeFuture extends StaticAnnotation
