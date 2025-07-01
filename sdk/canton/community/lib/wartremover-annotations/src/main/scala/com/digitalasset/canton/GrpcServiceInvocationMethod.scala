// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotation for methods and constructors. Implementations of such method (and any overrides) are
  * not checked. Neither are the arguments to calls of such a method.
  */
final class GrpcServiceInvocationMethod extends StaticAnnotation
