// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

/** dotnot(ation) is a simple library to implement a string-based dot-notation system
  * to access properties. A property is something in dot notation `branch1.branch2.leaf`
  * similar to the dot-notation in scala to reference a field.
  *
  * The idea is to represent all the possible paths from the root of the model down
  * to the leafs with a nice DSL.
  *
  * Naming:
  * - `Tree`: the model is seen as a `Tree` where each portion can either be a `Leaf` or a `Branch`
  * - `Leaf`: a node of the Tree that contains only one value (e.g. a primitive field, like `Int`)
  * - `Branch`: a non-leaf part of the model (e.g. an object with fields)
  * - `root`: the entry point and root of the Tree
  * - `path` or `property`: the list of all the pieces of the `Tree` from the `root` to a `leaf`
  * - `dotnot handler for k`: the set of all the paths and actions to be performed on an instance of `k`
  */
package dotnot {

  sealed trait DotNotFailure {
    def cursor: PropertyCursor
    def value: String
  }
  final case class CannotBeEmpty(target: String, cursor: PropertyCursor, value: String)
      extends DotNotFailure
  final case class MustBeLastPart(target: String, cursor: PropertyCursor, value: String)
      extends DotNotFailure
  final case class MustNotBeLastPart(target: String, cursor: PropertyCursor, value: String)
      extends DotNotFailure
  final case class TypeCoercionFailure(
      target: String,
      expectedType: String,
      cursor: PropertyCursor,
      value: String,
  ) extends DotNotFailure
  final case class UnknownProperty(target: String, cursor: PropertyCursor, value: String)
      extends DotNotFailure
  final case class MatchNotFoundForValue(target: String, cursor: PropertyCursor, value: String)
      extends DotNotFailure
  final case class UnknownType(name: String, cursor: PropertyCursor, value: String)
      extends DotNotFailure

  final case class NameMatcherToAction[T, R, C](matcher: NameMatcher, action: Action[T, R, C])
  final case class ValueMatcherToAction[T, R, C](matcher: ValueMatcher, action: Action[T, R, C])

  /** Utility for debugging purposes, implements an equalsTo function with a toString defined */
  final case class equalsTo[T](t: T) extends Function[T, Boolean] {
    def apply(t2: T): Boolean = t == t2
    override def toString: String = s"""_ == $t"""
  }

  final case object constTrue extends Function[String, Boolean] {
    def apply(s: String): Boolean = true
    override def toString: String = s"""_ => true"""
  }

  sealed trait OnTreeBase[T, R, C] {

    def target: String
    def onTree: OnTreeReady[T, R, C]

    def onLeafP(nameMatcher: NameMatcher): OnLeaf[T, R, C] =
      OnLeaf[T, R, C](onTree, nameMatcher)

    def onLeaf(name: String): OnLeaf[T, R, C] =
      onLeafP(equalsTo(name))

    /** Match a branch by predicate on the name and delegate handling the subtree from that branch
      *
      * @param nameMatcher matcher of the branch name
      * @param project the projection from the parent model of type `T` to the child model
      *                `P` where `P` is the type of the field of an instance of `T`
      * @param delegate the dotnot handler for the subtree
      */
    def onBranchP[P](
        nameMatcher: NameMatcher,
        project: T => P,
        delegate: OnTreeReady[P, R, C],
    ): OnTreeReady[T, R, C] = {
      val action: (T, PropertyCursor, String, C) => Either[DotNotFailure, R] =
        (t: T, cursor: PropertyCursor, value: String, context: C) => {
          cursor.next match {
            case None =>
              Left(MustNotBeLastPart(target, cursor, value))
            case Some(nextCursor) =>
              val p = project(t)
              delegate.run(p, nextCursor, value, context)
          }
        }
      val nameMatcherToAction = NameMatcherToAction[T, R, C](nameMatcher, action)
      onTree.copy(nameMatcherToActions = onTree.nameMatcherToActions :+ nameMatcherToAction)
    }

    /** Same as [[onBranchP]] but match the branch by name */
    def onBranch[P](
        name: String,
        project: T => P,
        delegate: OnTreeReady[P, R, C],
    ): OnTreeReady[T, R, C] =
      if (name == "*") {
        onBranchP[P](constTrue, project, delegate)
      } else {
        onBranchP[P](equalsTo(name), project, delegate)
      }

    /** Given a collection, match one the first element `e` with `getName(e)` equal to
      * the value currently pointed by the cursor and then delegate handling that element
      * to `delegate`
      *
      * @param getName a function to read the name of `E`
      * @param delegate the dotnot handler for `e`
      */
    def onElements[E](getName: E => String, delegate: OnTreeReady[E, R, C])(implicit
        ev: T <:< Iterable[E]
    ): OnTreeReady[T, R, C] = {
      val action: (T, PropertyCursor, String, C) => Either[DotNotFailure, R] =
        (t: T, cursor: PropertyCursor, value: String, context: C) => {
          cursor.next match {
            case None =>
              Left(MustNotBeLastPart(target, cursor, value))
            case Some(nextCursor) =>
              t.find(getName(_).toLowerCase == cursor.current.toLowerCase) match {
                case None =>
                  Left(UnknownProperty(target, cursor, value))
                case Some(e) =>
                  delegate.run(e, nextCursor, value, context)
              }
          }
        }
      val nameMatcherToAction = NameMatcherToAction[T, R, C](constTrue, action)
      onTree.copy(nameMatcherToActions = onTree.nameMatcherToActions :+ nameMatcherToAction)
    }

    def run(t: T, cursor: PropertyCursor, value: String, context: C): Either[DotNotFailure, R] = {
      onTree.nameMatcherToActions
        .find(_.matcher(cursor.current))
        .map(_.action)
        .orElse(onTree.default) match {
        case None =>
          Left(UnknownProperty(target, cursor, value))
        case Some(action) =>
          action(t, cursor, value, context)
      }
    }
  }

  final case class OnTree[T, R, C](target: String) extends OnTreeBase[T, R, C] {

    def onTree: OnTreeReady[T, R, C] =
      OnTreeReady[T, R, C](target, Vector(), None)

  }

  final case class OnTreeReady[T, R, C](
      target: String,
      nameMatcherToActions: Vector[NameMatcherToAction[T, R, C]],
      default: Option[Action[T, R, C]],
  ) extends OnTreeBase[T, R, C] {

    override def onTree: OnTreeReady[T, R, C] = this

  }

  sealed trait OnLeafBase[T, R, C] {
    def onTreeOld: OnTreeReady[T, R, C]
    def leafMatcher: NameMatcher
    def valueMatcherToActions: Vector[ValueMatcherToAction[T, R, C]]
    def default: Option[Action[T, R, C]]

    def onValueP(valueMatcher: ValueMatcher): OnValue[T, R, C] =
      OnValue[T, R, C](
        OnLeafReady(onTreeOld, leafMatcher, valueMatcherToActions, default),
        valueMatcher,
      )

    def onValue(value: String): OnValue[T, R, C] =
      onValueP(equalsTo(value))

    def onAnyValue: OnValue[T, R, C] =
      onValueP(constTrue)
  }

  final case class OnLeaf[T, R, C](onTreeOld: OnTreeReady[T, R, C], leafMatcher: NameMatcher)
      extends OnLeafBase[T, R, C] {
    override def valueMatcherToActions: Vector[ValueMatcherToAction[T, R, C]] = Vector()
    override def default: Option[Action[T, R, C]] = None
  }

  final case class OnLeafReady[T, R, C](
      onTreeOld: OnTreeReady[T, R, C],
      leafMatcher: NameMatcher,
      valueMatcherToActions: Vector[ValueMatcherToAction[T, R, C]],
      default: Option[Action[T, R, C]],
  ) extends OnLeafBase[T, R, C]
      with OnTreeBase[T, R, C] {

    override def target: String = onTreeOld.target

    private def nameAction: NameMatcherToAction[T, R, C] = {
      val action: Action[T, R, C] = (t: T, cursor: PropertyCursor, value: String, context: C) => {
        valueMatcherToActions.find(_.matcher(value)).map(_.action).orElse(default) match {
          case None =>
            Left(MatchNotFoundForValue(onTreeOld.target, cursor, value))
          case Some(action) =>
            action(t, cursor, value, context)
        }
      }
      NameMatcherToAction[T, R, C](leafMatcher, action)
    }

    private def onTreeUpdated: OnTreeReady[T, R, C] =
      onTreeOld.copy(nameMatcherToActions = onTreeOld.nameMatcherToActions :+ nameAction)

    override def onTree: OnTreeReady[T, R, C] = onTreeUpdated
  }

  final case class OnValue[T, R, C](onLeaf: OnLeafReady[T, R, C], valueMatcher: ValueMatcher) {

    def perform[K](f: (T, K) => R)(implicit readK: Read[K]): OnLeafReady[T, R, C] = {
      val action: Action[T, R, C] = (t: T, cursor: PropertyCursor, value: String, _: C) => {
        if (cursor.isLast) {
          Right(f(t, readK.from(value).toOption.get))
        } else {
          Left(MustBeLastPart(onLeaf.onTreeOld.target, cursor, value))
        }
      }
      val valueMatcherToAction = ValueMatcherToAction(valueMatcher, action)
      onLeaf.copy(valueMatcherToActions = onLeaf.valueMatcherToActions :+ valueMatcherToAction)
    }

    def const(r: R): OnLeafReady[T, R, C] =
      perform[String]((_: T, _: String) => r)
  }
}

package object dotnot {

  type NameMatcher = String => Boolean
  type ValueMatcher = String => Boolean
  type Action[T, R, C] = (T, PropertyCursor, String, C) => Either[DotNotFailure, R]

  /** Create a dot-notation handler for a type `T` with name `name` and with return
    * type from the leafs of type `R`
    */
  def root[T, R, C](name: String): OnTree[T, R, C] =
    OnTree[T, R, C](name)

  /** Utility method that creates a "opaque" dot-notation handler, which is
    * a handler that bypass the library and uses the given function `f` to
    * access the underlying structure of type `T` and to produce a result of
    * type `R`.
    *
    * Useful for when dotnot is not useful
    */
  def opaque[T, R, C](name: String)(f: Action[T, R, C]): OnTreeReady[T, R, C] =
    OnTreeReady[T, R, C](name, Vector(), Some(f))
}
