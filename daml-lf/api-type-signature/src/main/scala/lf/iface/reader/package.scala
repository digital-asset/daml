// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import com.daml.lf.typesig.{reader => tsr}

package object reader {
  @deprecated("moved to typesig.reader.DamlLfArchiveReader", since = "2.4.0")
  final val DamlLfArchiveReader = tsr.DamlLfArchiveReader

  @deprecated("moved to typesig.reader.Errors", since = "2.4.0")
  type Errors[K, A] = tsr.Errors[K, A]
  @deprecated("moved to typesig.reader.Errors", since = "2.4.0")
  final val Errors = tsr.Errors
  @deprecated("renamed to typesig.reader.SignatureReader", since = "2.4.0")
  final val InterfaceReader = tsr.SignatureReader
  @deprecated("renamed to typesig.reader.SignatureReaderMain", since = "2.4.0")
  final val InterfaceReaderMain = tsr.SignatureReaderMain
}
