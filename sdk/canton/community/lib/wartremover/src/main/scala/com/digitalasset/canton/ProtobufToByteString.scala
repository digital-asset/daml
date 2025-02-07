// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import scalapb.GeneratedMessage
import org.wartremover.{WartTraverser, WartUniverse}

/** Detects calls to `toByteString` on generated protobuf messages.
  *
  * @see https://github.com/DACH-NY/canton/blob/main/contributing/backwards-incompatible-changes.md
  */
object ProtobufToByteString extends WartTraverser {

  val message =
    "Protobuf messages should be converted into ByteStrings by going through Canton's version wrappers."

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val generatedMessageTypeSymbol =
      typeOf[GeneratedMessage].typeConstructor.typeSymbol
    require(generatedMessageTypeSymbol != NoSymbol)
    val toBytestringMethodName: TermName = TermName("toByteString")

    new u.Traverser {

      def isSubtypeOfGeneratedMessage(typ: Type): Boolean =
        typ.typeConstructor
          .baseType(generatedMessageTypeSymbol)
          .typeConstructor
          .typeSymbol == generatedMessageTypeSymbol

      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          case Select(receiver, method)
              if (method == toBytestringMethodName) &&
                receiver.tpe != null && isSubtypeOfGeneratedMessage(receiver.tpe) =>
            error(u)(tree.pos, message)

          case _ =>
            super.traverse(tree)
        }
    }
  }
}
