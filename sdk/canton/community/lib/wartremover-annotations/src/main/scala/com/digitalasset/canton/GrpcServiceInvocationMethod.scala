// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import scala.annotation.StaticAnnotation

/** Annotation for methods and constructors. Implementations of such method (and any overrides) are
  * not checked. Neither are the arguments to calls of such a method.
  */
final class GrpcServiceInvocationMethod extends StaticAnnotation
