// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface

import com.digitalasset.daml.lf.{iface => parent}

package object reader {
  @deprecated("import from parent `iface` package instead", since = "0.12.12")
  type InterfaceType = parent.InterfaceType
  @deprecated("import from parent `iface` package instead", since = "0.12.12")
  val InterfaceType = parent.InterfaceType
  @deprecated("import from parent `iface` package instead", since = "0.12.12")
  type Interface = parent.Interface
  @deprecated("import from parent `iface` package instead", since = "0.12.12")
  val Interface = parent.Interface
}
