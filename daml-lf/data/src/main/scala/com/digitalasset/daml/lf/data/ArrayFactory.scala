// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.daml.lf.data

import scala.reflect.ClassTag

final class ArrayFactory[T](implicit classTag: ClassTag[T]) {

  def apply(xs: T*): Array[T] = xs.toArray

  def ofDim(n: Int): Array[T] = Array.ofDim(n)

  val empty: Array[T] = ofDim(0)
}
