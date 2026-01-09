// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import _root_.scalapb.GeneratedMessage
import org.wartremover.{WartTraverser, WartUniverse}

/** Detects calls to `toByteString` on generated protobuf messages.
  *
  * @see
  *   https://github.com/DACH-NY/canton/blob/main/contributing/backwards-incompatible-changes.md
  */
object ProtobufToByteString extends WartTraverser {

  val message =
    "Protobuf messages should be converted into ByteStrings by going through Canton's version wrappers."

  override def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          if tree.isExpr then
            tree.asExpr match
              case '{ ($r: GeneratedMessage).toByteString } => error(tree.pos, message)
              case _ => ()
          super.traverseTree(tree)(owner)
    }
}
