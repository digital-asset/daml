// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.discard.Implicits.DiscardOps
import pprint.Tree

/** This object contains helper methods to fix ansi-escape sequences in a pretty-printed string to
  * help debug the issue: https://github.com/DACH-NY/canton/issues/17834 This allows to print a
  * `Tree` containing invalid 0x1B or 0x9B char, so that we can identify where and how it appears,
  * so that we can do a proper fix (pretty-printing unsafe strings in a canton's class
  * `PrettyPrinting` implementation).
  */
private[pretty] object AnsiEscapeFix {
  private def fansiStrAnsiEscapeFallbackToStar(s: String): String =
    try {
      // This line can produce an exception if the string contains unknown ansi-escape sequence,
      // i.e. 0x1B or 0x9B character at an unexpected position
      fansi.Str(s).discard
      s
    } catch {
      case err: IllegalArgumentException if err.getMessage contains ("Unknown ansi-escape") =>
        s.replace('\u001b', '*').replace('\u009b', '*')
    }

  def fixAnsiEscape(tree: Tree): Tree =
    tree match {
      case Tree.Literal(body) =>
        Tree.Literal(fansiStrAnsiEscapeFallbackToStar(body))
      case Tree.Lazy(fct) =>
        Tree.Lazy(ctx =>
          fct(ctx).map { x =>
            fansiStrAnsiEscapeFallbackToStar(x)
          }
        )
      case Tree.Infix(left, op, right) =>
        Tree.Infix(
          fixAnsiEscape(left),
          op,
          fixAnsiEscape(right),
        )
      case Tree.Apply(name, args) =>
        Tree.Apply(name, args.map(fixAnsiEscape))
      case Tree.KeyValue(key, value) =>
        Tree.KeyValue(key, fixAnsiEscape(value))
    }
}
