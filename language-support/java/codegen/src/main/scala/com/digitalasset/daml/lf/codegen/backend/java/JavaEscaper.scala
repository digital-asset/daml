// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java

import java.util

object JavaEscaper {

  def escapeString(str: String): String = {
    val escaped = escapeReservedJavaKeywords(str)
    if (escaped == str) { // run the dollar escape only if str is not a keyword
      escapeDollar(str)
    } else escaped
  }

  // tableswitch is not possible with strings but we still want fast string lookup for
  // keywords. To achieve it we use this hashmap
  val reservedJavaKeywords: java.util.HashSet[String] = {
    val set = new util.HashSet[String]()
    set.add("abstract")
    set.add("continue")
    set.add("for")
    set.add("new")
    set.add("switch")
    set.add("assert")
    set.add("default")
    set.add("goto")
    set.add("package")
    set.add("synchronized")
    set.add("boolean")
    set.add("do")
    set.add("if")
    set.add("private")
    set.add("this")
    set.add("break")
    set.add("double")
    set.add("implements")
    set.add("protected")
    set.add("throw")
    set.add("byte")
    set.add("else")
    set.add("import")
    set.add("public")
    set.add("throws")
    set.add("case")
    set.add("enum")
    set.add("instanceof")
    set.add("return")
    set.add("transient")
    set.add("catch")
    set.add("extends")
    set.add("int")
    set.add("short")
    set.add("try")
    set.add("char")
    set.add("final")
    set.add("interface")
    set.add("static")
    set.add("void")
    set.add("class")
    set.add("finally")
    set.add("long")
    set.add("strictfp")
    set.add("volatile")
    set.add("const")
    set.add("float")
    set.add("native")
    set.add("super")
    set.add("while")
    set.add("null") // not a keyword but a literal that needs to be escaped
    set
  }

  def escapeReservedJavaKeywords(str: String): String = {
    if (reservedJavaKeywords.contains(str)) str + "$"
    else str
  }

  def escapeDollar(str: String): String = str.replaceAll("\\$", "\\$\\$")

}
