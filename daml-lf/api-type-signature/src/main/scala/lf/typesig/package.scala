// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import data.Ref

// Types to be used internally
package object iface {

  type FieldWithType = (Ref.Name, Type)

  private[iface] type GetterSetterAt[-I, S, A] = (S, I) => Option[(A, A => S)]

  private[iface] type SetterAt[-I, S, A] = (S, I) => Option[(A => A) => S]

  private[iface] def lfprintln(
      @deprecated("shut up unused arguments warning", "") s: => String
  ): Unit = ()
}
