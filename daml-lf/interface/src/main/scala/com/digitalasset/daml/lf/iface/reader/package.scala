// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import com.daml.lf.{iface => parent}

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
