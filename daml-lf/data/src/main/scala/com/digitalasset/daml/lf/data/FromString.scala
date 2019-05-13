// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.daml.lf.data

abstract class FromString {

  type T

  def fromString(s: String): Either[String, T]

  @throws[IllegalArgumentException]
  final def assertFromString(s: String): T =
    assert(fromString(s))

  final def fromUtf8String(s: Utf8String): Either[String, T] =
    fromString(s.javaString)

  @throws[IllegalArgumentException]
  final def assertFromUtf8String(s: Utf8String): T =
    assert(fromUtf8String(s))

}
