// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import data.Ref

// Types to be used internally
package object typesig {

  type FieldWithType = (Ref.Name, Type)

  private[typesig] type GetterSetterAt[-I, S, A] = (S, I) => Option[(A, A => S)]

  private[typesig] type SetterAt[-I, S, A] = (S, I) => Option[(A => A) => S]

  private[typesig] def lfprintln(
      @deprecated("shut up unused arguments warning", "") s: => String
  ): Unit = ()
}
