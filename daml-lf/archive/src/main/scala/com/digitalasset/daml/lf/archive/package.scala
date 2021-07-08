// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

package object archive {

  @deprecated("use Error", since = "1.16.0")
  val Errors = Error

  @deprecated("use Error.Parsing", since = "1.16.0")
  val ParseError = Error.Parsing

}
