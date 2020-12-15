// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import com.daml.lf.language.Ast.Type

private[benchmark] final case class TypedValue[A](value: A, valueType: Type) {
  def mapValue[B](f: A => B): TypedValue[B] = TypedValue(f(value), valueType)
}
