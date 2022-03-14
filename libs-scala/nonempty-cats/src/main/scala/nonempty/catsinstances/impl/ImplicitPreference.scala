// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty
package catsinstances.impl

sealed abstract class ImplicitPreferenceModule {
  type T[+A]
  def apply[A, B](a: A): A with T[B]
}

object ImplicitPreferenceModule {
  private[impl] object Module extends ImplicitPreferenceModule {
    type T[+A] = Any
    override def apply[A, B](a: A) = a
  }
}
