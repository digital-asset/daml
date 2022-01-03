// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import java.{util => j}

import com.daml.lf.data.Ref

// Types to be used internally
package object iface {

  type FieldWithType = (Ref.Name, Type)

  private[iface] def lfprintln(
      @deprecated("shut up unused arguments warning", "") s: => String
  ): Unit = ()

  private[iface] def toOptional[A](o: Option[A]): j.Optional[A] =
    o.fold(j.Optional.empty[A])(j.Optional.of)
}
