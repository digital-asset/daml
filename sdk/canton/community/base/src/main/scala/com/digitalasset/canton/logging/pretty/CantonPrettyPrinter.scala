// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.logging.pretty.Pretty.{
  DefaultEscapeUnicode,
  DefaultIndent,
  DefaultShowFieldNames,
  DefaultWidth,
}
import com.digitalasset.canton.util.ErrorUtil
import com.google.protobuf.ByteString
import pprint.{PPrinter, Tree}

/** Adhoc pretty printer to nicely print the full structure of a class that does not have an explicit pretty definition */
class CantonPrettyPrinter(maxStringLength: Int, maxMessageLines: Int) {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def printAdHoc(message: Any): String =
    message match {
      case null => ""
      case product: Product =>
        try {
          pprinter(product).toString
        } catch {
          case err: IllegalArgumentException => ErrorUtil.messageWithStacktrace(err)
        }
      case _: Any =>
        import com.digitalasset.canton.logging.pretty.Pretty.*
        message.toString.limit(maxStringLength).toString
    }

  private lazy val pprinter: PPrinter = PPrinter.BlackWhite.copy(
    defaultWidth = DefaultWidth,
    defaultHeight = maxMessageLines,
    defaultIndent = DefaultIndent,
    defaultEscapeUnicode = DefaultEscapeUnicode,
    defaultShowFieldNames = DefaultShowFieldNames,
    additionalHandlers = {
      case _: ByteString => Tree.Literal("ByteString")
      case s: String =>
        import com.digitalasset.canton.logging.pretty.Pretty.*
        s.limit(maxStringLength).toTree
      case Some(p) =>
        pprinter.treeify(
          p,
          escapeUnicode = DefaultEscapeUnicode,
          showFieldNames = DefaultShowFieldNames,
        )
      case Seq(single) =>
        pprinter.treeify(
          single,
          escapeUnicode = DefaultEscapeUnicode,
          showFieldNames = DefaultShowFieldNames,
        )
    },
  )

}
