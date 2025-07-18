// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotation for computation transformer type constructors (e.g., a monad transformer) so that if
  * it will be treated future-like when applied to a future-like computation type.
  *
  * @param transformedTypeArgumentPosition
  *   The type argument position for the computation type that is transformed
  */
final case class FutureTransformer(transformedTypeArgumentPosition: Int) extends StaticAnnotation
