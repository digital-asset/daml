// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.v1.value.Value.{Sum => VSum}

abstract class VoidValueRef extends ValueRef {
  val cannotExist: Nothing
}

object VoidValueRef {

  /** Automatically provides the [[Value]] instance for all subclasses of
    * [[VoidValueRef]], which are produced by codegen for zero-constructor
    * variants.
    */
  implicit def `VoidValueRef Value`[A <: VoidValueRef]: Value[A] =
    new Value.InternalImpl[A] {
      override def read(argumentValue: VSum): Option[A] = None
      override def write(obj: A): VSum = obj.cannotExist
    }
}
