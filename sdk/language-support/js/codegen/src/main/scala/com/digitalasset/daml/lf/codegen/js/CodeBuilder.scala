// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import scala.collection.mutable

private[codegen] class CodeBuilder() {
  private val builder = new mutable.StringBuilder()
  private var indentation = ""

  // whether the current code is printed inside an existing line,
  // e.g. 'foo = ${current code is printed here};'
  private var inline = false

  def addLine(line: String): this.type = {
    add(line)
    // if the current code is inlined, we don't add any line break
    if (!inline) builder.append('\n')
    this
  }

  def addInline(open: String, close: String)(inlined: => Unit): Unit = {
    withInline(true) {
      add(open)
      inlined
    }
    addLine(close)
  }

  def addBlock(open: String, close: String)(block: => Unit): Unit = {
    withInline(false) {
      addLine(open)
      withIndentation(block)
    }
    if (close.nonEmpty) addLine(close)
  }

  def addEmptyLine(): Unit = builder.append('\n')

  private def add(code: String): this.type = {
    if (builder.lastOption.contains('\n') && !code.startsWith("\n")) {
      builder.append(indentation)
    }
    builder.append(code)
    this
  }

  override def toString = builder.toString

  private def withInline(value: Boolean)(code: => Unit) = {
    val prev = inline
    inline = value
    code
    inline = prev
  }
  private def withIndentation(code: => Unit) = {
    indentation = indentation + "  "
    code
    indentation = indentation.drop(2)
  }
}
