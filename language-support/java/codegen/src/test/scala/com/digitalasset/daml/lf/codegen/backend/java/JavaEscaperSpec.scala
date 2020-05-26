// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import org.scalatest.{FlatSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class JavaEscaperSpec extends FlatSpec with Matchers {

  behavior of "JavaEscaper.escapeString"

  it should "return the empty string untouched" in {
    JavaEscaper.escapeString("") shouldEqual ""
  }

  // TODO(mp): use property-based testing and merge with the empty string example
  it should "not escape strings that are not java reserved keyword and that don't contain dollars" in {
    JavaEscaper.escapeString("mrhouse") shouldEqual "mrhouse"
  }

  // TODO(mp): use property-based testing
  it should "escape keywords" in {
    JavaEscaper.escapeString("boolean") shouldEqual "boolean$"
  }

  // TODO(mp): use property-based testing
  it should "escape dollars in DAML identifiers" in {
    JavaEscaper.escapeString("foo$") shouldEqual "foo$$"
  }
}
