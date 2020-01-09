// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

package object backend {

  private[codegen] val backends = Map(
    "java" -> java.JavaBackend
  )

  private[codegen] def values: Iterable[String] = backends.keys

  private[codegen] def lookupBackend(name: String): Option[Backend] = backends.get(name.toLowerCase)

  private[codegen] val read: scopt.Read[Backend] = scopt.Read.stringRead.map(
    str => lookupBackend(str).getOrElse(throw new UnknownBackend(str))
  )
}
