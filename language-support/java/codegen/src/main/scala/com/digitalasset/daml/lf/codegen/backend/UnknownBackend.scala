// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend

import com.daml.lf.codegen.backend

private[codegen] class UnknownBackend(backendName: String)
    extends RuntimeException(
      UnknownBackend.message(backendName, backend.backends.keys),
      null,
      true,
      false
    )

private[codegen] object UnknownBackend {
  def message(backendName: String, validBackendNames: Iterable[String]): String =
    s"Unknown backend '$backendName', " ++ (
      if (validBackendNames.size <= 0) ""
      else if (validBackendNames.size == 1) s"expected '${validBackendNames.mkString}'"
      else s"expected on of ${validBackendNames.map(s => s"'$s'").mkString("[", ", ", "]")}"
    )
}
