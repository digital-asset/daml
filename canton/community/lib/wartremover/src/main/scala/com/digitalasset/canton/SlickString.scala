// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}
import slick.jdbc.PositionedParameters

import scala.reflect.NameTransformer

/** The DB may truncate strings of unbounded length. Therefore, this wart flags the following:
  * <ul>
  *   <li>Calls to [[slick.jdbc.PositionedParameters.setString]]</li>
  *   <li>Calls to [[slick.jdbc.PositionedParameters.setStringOption]]</li>
  *   <li>References to [[slick.jdbc.SetParameter.SetString]]</li>
  *   <li>References to [[slick.jdbc.SetParameter.SetStringOption]]</li>
  * </ul>
  * This includes references generated by the string interpolators `sql` and `sqlu`
  */
object SlickString extends WartTraverser {

  val message: String =
    "The DB may truncate strings of unbounded length. Use a LengthLimitedString instead."

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val positionedParameterSymbol = typeOf[PositionedParameters]
    val positionedParameterTypeSymbol = positionedParameterSymbol.typeSymbol
    require(positionedParameterTypeSymbol != NoSymbol)

    val setStringMethodName: TermName = TermName("setString")
    val setStringMethod = positionedParameterSymbol.member(setStringMethodName)
    require(setStringMethod != NoSymbol)

    val setStringOptionMethodName: TermName = TermName("setStringOption")
    val setStringOptionMethod = positionedParameterSymbol.member(setStringOptionMethodName)
    require(setStringOptionMethod != NoSymbol)

    val setParameterMethodName: TermName = TermName(NameTransformer.encode(">>"))
    val setParameterMethod = positionedParameterSymbol.member(setParameterMethodName)
    require(setParameterMethod != NoSymbol)

    val setStringObject = rootMirror.staticModule("slick.jdbc.SetParameter.SetString")
    val setStringOptionObject = rootMirror.staticModule("slick.jdbc.SetParameter.SetStringOption")

    val predefSymbol = typeOf[Predef.type]
    val implicitlyMethodName: TermName = TermName("implicitly")
    val implicitlyMethod = predefSymbol.member(implicitlyMethodName)
    require(implicitlyMethod != NoSymbol)

    new Traverser {
      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          // References to SetParameter.SetString
          case t if t.symbol == setStringObject =>
            error(u)(tree.pos, message)
            super.traverse(tree)
          // References to SetParameter.SetStringOption
          case t if t.symbol == setStringOptionObject =>
            error(u)(tree.pos, message)
            super.traverse(tree)
          // PositionedParameter.setString and PositionedParameter.setStringOption
          case Apply(Select(receiver, methodName), _)
              if receiver.tpe.typeSymbol == positionedParameterTypeSymbol &&
                (methodName == setStringMethodName || methodName == setStringOptionMethodName) =>
            error(u)(tree.pos, message)
            super.traverse(tree)
          case _ =>
            super.traverse(tree)
        }
      }
    }
  }
}
