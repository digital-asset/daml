// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.exception

/** The reason why a given type's code can't be generated
  */
final case class UnsupportedTypeError(msg: String)

object UnsupportedTypeError {

  def apply[K](id: K, missing: List[K]): UnsupportedTypeError = {
    val missingString = missing.mkString("'", "', '", "'")
    UnsupportedTypeError(
      s"Type $id is not supported as dependencies have unsupported type: $missingString."
    )
  }

}
