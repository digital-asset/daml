// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import cats.syntax.functorFilter.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.pretty.PrettyUtil.nullTree
import pprint.{Tree, Walker}

import scala.annotation.tailrec
import scala.reflect.ClassTag

/** Utility methods for constructing [[Pretty]] instances.
  */
trait PrettyUtil {

  import Pretty.PrettyOps

  /** A tree representing the type name and parameter trees.
    */
  def prettyOfClass[T](getParamTrees: (T => Option[Tree])*): Pretty[T] =
    inst => {
      // getSimpleName on an anonymous class returns an empty string
      // so we search for the first non-anonymous superclass.
      //
      // To guard against the possibility that the direct superclass of an anonymous
      // class is again anonymous (not sure whether this can happen in Scala)
      // we go up in the hierarchy until we find a non-anonymous class.
      @tailrec
      def firstNonAnonmymousSuperclass(clazz: Class[_]): Class[_] =
        if (clazz.isAnonymousClass) {
          // The superclass cannot be `null` as neither java.lang.Object
          // nor Java interfaces nor primitive types nor void are anonymous classes.
          firstNonAnonmymousSuperclass(clazz.getSuperclass)
        } else clazz

      if (inst == null) nullTree
      else {
        val simpleName = firstNonAnonmymousSuperclass(inst.getClass).getSimpleName
        Tree.Apply(simpleName, getParamTrees.mapFilter(_(inst)).iterator)
      }
    }

  /** A tree presenting the type name only. (E.g., for case objects.)
    */
  def prettyOfObject[T <: Product]: Pretty[T] = inst =>
    if (inst == null) nullTree else treeOfString(inst.productPrefix)

  /** A tree consisting of a labelled node with the given children. */
  def prettyNode[T](label: String, children: (T => Option[Tree])*): Pretty[T] =
    inst => Tree.Apply(label, children.mapFilter(_(inst)).iterator)

  /** A tree representing both parameter name and value.
    */
  def param[T, V: Pretty](
      name: String,
      getValue: T => V,
      cond: T => Boolean = (_: T) => true,
  ): T => Option[Tree] =
    conditionalParam[T, V](getValue, cond, value => mkNameValue(name, value.toTree))

  /** A tree only written if not matching the default value */
  def paramIfNotDefault[T, V: Pretty](
      name: String,
      getValue: T => V,
      default: V,
  ): T => Option[Tree] =
    param(name, getValue, getValue(_) != default)

  private def conditionalParam[T, V](
      getValue: T => V,
      cond: T => Boolean,
      resultOfValueTree: V => Tree,
  ): T => Option[Tree] =
    inst =>
      if (cond(inst)) {
        Some(resultOfValueTree(getValue(inst)))
      } else {
        None
      }

  def prettyInfix[T] = new PrettyUtil.PrettyInfixPartiallyApplied[T](false)

  private def mkNameValue(name: String, valueTree: Tree): Tree =
    Tree.Infix(treeOfString(name), "=", valueTree)

  def paramIfNonEmpty[T, V <: IterableOnce[_]: Pretty](
      name: String,
      getValue: T => V,
  ): T => Option[Tree] =
    param(name, getValue, getValue(_).iterator.nonEmpty)

  def paramIfDefined[T, V: Pretty](name: String, getValue: T => Option[V]): T => Option[Tree] =
    getValue(_).map(value => mkNameValue(name, value.toTree))

  def paramIfTrue[T](label: String, getValue: T => Boolean): T => Option[Tree] =
    customParam(_ => label, getValue)

  /** A tree representing a parameter value without a parameter name.
    */
  def unnamedParam[T, V: Pretty](
      getValue: T => V,
      cond: T => Boolean = (_: T) => true,
  ): T => Option[Tree] =
    conditionalParam[T, V](getValue, cond, _.toTree)

  def unnamedParamIfNonEmpty[T, V <: IterableOnce[_]: Pretty](getValue: T => V): T => Option[Tree] =
    unnamedParam(getValue, getValue(_).iterator.nonEmpty)

  def unnamedParamIfDefined[T, V: Pretty](getValue: T => Option[V]): T => Option[Tree] =
    getValue(_).map(value => value.toTree)

  /** A tree representing a parameter name without a parameter value.
    * Use this for parameters storing confidential or binary data.
    */
  def paramWithoutValue[T](name: String, cond: T => Boolean = (_: T) => true): T => Option[Tree] =
    conditionalParam(_ => treeOfString("..."), cond, mkNameValue(name, _))

  /** Use this if you need a custom representation of a parameter.
    * Do not use this to create lengthy strings, as line wrapping is not supported.
    */
  def customParam[T](
      getValue: T => String,
      cond: T => Boolean = (_: T) => true,
  ): T => Option[Tree] =
    conditionalParam(getValue, cond, treeOfString)

  /** Use this to indicate that you've omitted fields from pretty printing */
  def indicateOmittedFields[T]: T => Option[Tree] =
    customParam(_ => "...")

  /** Use this to give a class with a singleton parameter the same pretty representation as the parameter.
    */
  def prettyOfParam[T, V: Pretty](getValue: T => V): Pretty[T] = inst =>
    if (inst == null) nullTree else getValue(inst).toTree

  /** Creates a pretty instance from a string function.
    * Do not use this with lengthy strings, as line wrapping is not supported.
    */
  def prettyOfString[T](toString: T => String): Pretty[T] = inst =>
    if (inst == null) nullTree else treeOfString(toString(inst))

  private def treeOfString(s: String): Tree =
    if (s.isEmpty) {
      // Note that the parameter of `Literal` must be non-empty.
      Tree.Literal("\"\"")
    } else {
      Tree.Literal(s)
    }

  /** Use this as a temporary solution, to make the code compile during an ongoing migration.
    * Drawbacks:
    * <ul>
    * <li> Instances of `Pretty[T]` are ignored.</li>
    * <li> No parameter names</li>
    * </ul>
    */
  def adHocPrettyInstance[T <: Product](implicit c: ClassTag[T]): Pretty[T] =
    // Need to restrict to Product subtypes as the Walker cannot faithfully deal with arbitrary types.
    new Walker {
      override def additionalHandlers: PartialFunction[Any, Tree] = {
        case p: PrettyPrinting if !c.runtimeClass.isInstance(p) => p.pretty.treeOf(p)
        case p: Product if p.productArity == 0 => treeOfString(p.productPrefix)
      }
    }.treeify(
      _,
      escapeUnicode = Pretty.DefaultEscapeUnicode,
      showFieldNames = Pretty.DefaultShowFieldNames,
    )
}

object PrettyUtil extends PrettyUtil {
  private[pretty] final class PrettyInfixPartiallyApplied[T](private val dummy: Boolean)
      extends AnyVal {
    def apply[U: Pretty, V: Pretty](first: T => U, infixOp: String, second: T => V): Pretty[T] = {
      inst =>
        import Pretty.PrettyOps
        if (inst == null) nullTree
        else Tree.Infix(first(inst).toTree, infixOp, second(inst).toTree)
    }
  }

  /** How to pretty-print `null` values. This is consistent with [[pprint.Walker.treeify]] */
  private[pretty] val nullTree = Tree.Literal("null")
}
