// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.{Decimal, ImmArray, InsertOrdSet, Ref}
import com.daml.lf.language.Ast._

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

object Util {

  object TTyConApp {
    def apply(con: Ref.TypeConName, args: ImmArray[Type]): Type =
      args.foldLeft[Type](TTyCon(con))((typ, arg) => TApp(typ, arg))
    def unapply(typ: Type): Option[(Ref.TypeConName, ImmArray[Type])] = {
      @tailrec
      def go(typ: Type, targs: List[Type]): Option[(Ref.TypeConName, ImmArray[Type])] =
        typ match {
          case TApp(tfun, targ) => go(tfun, targ :: targs)
          case TTyCon(con) => Some((con, ImmArray(targs)))
          case _ => None
        }
      go(typ, Nil)
    }
  }

  object TFun extends ((Type, Type) => Type) {
    def apply(targ: Type, tres: Type) =
      TApp(TApp(TBuiltin(BTArrow), targ), tres)
  }

  class ParametricType1(bType: BuiltinType) {
    val cons = TBuiltin(bType)
    def apply(typ: Type): Type =
      TApp(cons, typ)
    def unapply(typ: TApp): Option[Type] = typ match {
      case TApp(`cons`, elemType) => Some(elemType)
      case _ => None
    }
  }

  class ParametricType2(bType: BuiltinType) {
    val cons = TBuiltin(bType)
    def apply(typ1: Type, typ2: Type): Type =
      TApp(TApp(cons, typ1), typ2)
    def unapply(typ: TApp): Option[(Type, Type)] = typ match {
      case TApp(TApp(`cons`, type1), type2) => Some(type1 -> type2)
      case _ => None
    }
  }

  val TUnit = TBuiltin(BTUnit)
  val TBool = TBuiltin(BTBool)
  val TInt64 = TBuiltin(BTInt64)
  val TText = TBuiltin(BTText)
  val TTimestamp = TBuiltin(BTTimestamp)
  val TDate = TBuiltin(BTDate)
  val TParty = TBuiltin(BTParty)
  val TAny = TBuiltin(BTAny)
  val TTypeRep = TBuiltin(BTTypeRep)

  val TNumeric = new ParametricType1(BTNumeric)
  val TList = new ParametricType1(BTList)
  val TOptional = new ParametricType1(BTOptional)
  val TTextMap = new ParametricType1(BTTextMap)
  val TGenMap = new ParametricType2(BTGenMap)
  val TUpdate = new ParametricType1(BTUpdate)
  val TScenario = new ParametricType1(BTScenario)
  val TContractId = new ParametricType1(BTContractId)

  val TParties = TList(TParty)
  val TDecimalScale = TNat(Decimal.scale)
  val TDecimal = TNumeric(TDecimalScale)

  val EUnit = EPrimCon(PCUnit)
  val ETrue = EPrimCon(PCTrue)
  val EFalse = EPrimCon(PCFalse)

  def EBool(b: Boolean): EPrimCon = if (b) ETrue else EFalse

  val CPUnit = CPPrimCon(PCUnit)
  val CPTrue = CPPrimCon(PCTrue)
  val CPFalse = CPPrimCon(PCFalse)

  type Graph[X] = Map[X, Set[X]]

  // Topologically order the vertices of an abstract Graph.
  // If the `graph` is a directed acyclic graph returns a list of its vertices in topological order as `Right`
  // otherwise returns a list of vertices that form a cycle as `Left`
  def topoSort[X](graph: Graph[X]): Either[List[X], List[X]] = {

    var white = graph.keySet
    var black = graph.values.foldLeft(InsertOrdSet.empty[X])(_ | _.filterNot(white))
    def gray(x: X) = !white(x) && !black(x)

    def visitSet(xs: Set[X]): Option[X] = xs.foldLeft(Option.empty[X])(_ orElse visit(_))

    def visit(x: X): Option[X] =
      if (black(x))
        None
      else if (!white(x))
        Some(x)
      else { white -= x; visitSet(graph(x)) } orElse { black += x; None }

    def buildCycle(curr: X, start: X, list: List[X] = List.empty): List[X] = {
      val next = graph(curr).find(gray).getOrElse(throw new UnknownError)
      if (next == start)
        curr :: list
      else
        buildCycle(next, start, curr :: list)
    }

    visitSet(graph.keySet).fold[Either[List[X], List[X]]](Right(black.toList))(x =>
      Left(buildCycle(x, x)))
  }

  // Return the `pkgIds` and all its dependencies in topological order.
  // A package undefined w.r.t. the function `packages` is treated as a sink.
  def dependenciesInTopologicalOrder(
      pkgIds: List[Ref.PackageId],
      packages: Ref.PackageId PartialFunction Package,
  ): List[Ref.PackageId] = {

    @tailrec
    def buildGraph(
        toProcess0: List[Ref.PackageId] = pkgIds,
        seen0: Set[Ref.PackageId] = pkgIds.toSet,
        graph0: Graph[Ref.PackageId] = HashMap.empty,
    ): Graph[Ref.PackageId] =
      toProcess0 match {
        case pkgId :: toProcess1 =>
          val deps = packages.lift(pkgId).fold(Set.empty[Ref.PackageId])(_.directDeps)
          val newDeps = deps.filterNot(seen0)
          buildGraph(
            newDeps.foldLeft(toProcess1)(_.::(_)),
            seen0 ++ newDeps,
            graph0.updated(pkgId, deps)
          )
        case Nil => graph0
      }

    topoSort(buildGraph()).fold(
      // If we get a cycle in package dependencies, there is something very wrong
      // (i.e. we find a collision in SHA256), so we crash.
      ids => throw new Error(s"cycle in package definitions ${ids.mkString(" -> ")}"),
      identity
    )
  }

}
