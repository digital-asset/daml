// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import com.daml.lf.typesig.{reader => tsr}

// @deprecated("package moved to com.daml.lf.typesig.reader", since = "2.4.0")
package object reader {
  // @deprecated("moved to typesig.reader.DamlLfArchiveReader", since = "2.4.0")
  final val DamlLfArchiveReader = tsr.DamlLfArchiveReader

  // @deprecated("moved to typesig.reader.Errors", since = "2.4.0")
  type Errors[K, A] = tsr.Errors[K, A]
  // @deprecated("moved to typesig.reader.Errors", since = "2.4.0")
  final val Errors = tsr.Errors
  // @deprecated("moved to typesig.reader.InterfaceReader", since = "2.4.0")
  final val InterfaceReader = tsr.InterfaceReader
  // @deprecated("moved to typesig.reader.InterfaceReaderMain", since = "2.4.0")
  final val InterfaceReaderMain = tsr.InterfaceReaderMain
}
