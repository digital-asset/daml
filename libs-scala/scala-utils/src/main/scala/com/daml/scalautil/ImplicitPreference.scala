// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

sealed abstract class ImplicitPreferenceModule {
  type T[+A]
  def apply[A, B](a: A): A with T[B]
}

object ImplicitPreferenceModule {
  private[scalautil] object Module extends ImplicitPreferenceModule {
    type T[+A] = Any
    override def apply[A, B](a: A) = a
  }
}
