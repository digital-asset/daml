// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class JavaEscaperSpec extends AnyFlatSpec with Matchers {

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
  it should "escape dollars in Daml identifiers" in {
    JavaEscaper.escapeString("foo$") shouldEqual "foo$$"
  }
}
