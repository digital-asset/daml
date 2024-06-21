// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast._
import com.daml.nameof.NameOf

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
          case TTyCon(con) => Some((con, targs.to(ImmArray)))
          case _ => None
        }
      go(typ, Nil)
    }
  }

  object TTVarApp {
    def apply(name: Ast.TypeVarName, args: ImmArray[Type]): Type =
      args.foldLeft[Type](TVar(name))((typ, arg) => TApp(typ, arg))
    def unapply(typ: Type): Option[(Ast.TypeVarName, ImmArray[Type])] = {
      @tailrec
      def go(typ: Type, targs: List[Type]): Option[(Ast.TypeVarName, ImmArray[Type])] =
        typ match {
          case TApp(tfun, targ) => go(tfun, targ :: targs)
          case TVar(name) => Some((name, targs.to(ImmArray)))
          case _ => None
        }
      go(typ, Nil)
    }
  }

  object TFun extends ((Type, Type) => Type) {
    def apply(targ: Type, tres: Type) =
      TApp(TApp(TBuiltin(BTArrow), targ), tres)
    def unapply(typ: Type): Option[(Type, Type)] = typ match {
      case TApp(TApp(TBuiltin(BTArrow), targ), tres) => Some((targ, tres))
      case _ => None
    }
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
  val TBigNumeric = TBuiltin(BTBigNumeric)
  val TRoundingMode = TBuiltin(BTRoundingMode)

  val TNumeric = new ParametricType1(BTNumeric)
  val TList = new ParametricType1(BTList)
  val TOptional = new ParametricType1(BTOptional)
  val TTextMap = new ParametricType1(BTTextMap)
  val TGenMap = new ParametricType2(BTGenMap)
  val TUpdate = new ParametricType1(BTUpdate)
  val TScenario = new ParametricType1(BTScenario)
  val TContractId = new ParametricType1(BTContractId)

  val TDecimal = TNumeric(TNat.values(10))
  val TParties = TList(TParty)

  val TAnyException = TBuiltin(BTAnyException)

  val EUnit = EBuiltinCon(BCUnit)
  val ETrue = EBuiltinCon(BCTrue)
  val EFalse = EBuiltinCon(BCFalse)

  val EEmptyString = EBuiltinLit(BLText(""))

  def EBool(b: Boolean): EBuiltinCon = if (b) ETrue else EFalse

  val CPUnit = CPBuiltinCon(BCUnit)
  val CPTrue = CPBuiltinCon(BCTrue)
  val CPFalse = CPBuiltinCon(BCFalse)

  @tailrec
  def destructETyApp(e: Expr, targs: List[Type] = List.empty): (Expr, List[Type]) =
    e match {
      case ETyApp(e, t) => destructETyApp(e, t :: targs)
      case _ => (e, targs)
    }

  @tailrec
  def destructApp(typ: Type, tyArgs: List[Type] = List.empty): (Type, List[Type]) =
    typ match {
      case TApp(tyFun, tyArg) => destructApp(tyFun, tyArg :: tyArgs)
      case otherwise => (otherwise, tyArgs)
    }

  // this is not tail recursive, but it doesn't really matter, since types are bounded
  // by what's in the source, which should be short enough...
  @throws[IllegalArgumentException]
  def substitute(typ: Type, subst: Iterable[(TypeVarName, Type)]): Type = {

    def go(typ: Type, subst: Map[TypeVarName, Type]): Type =
      typ match {
        case TVar(v) => subst.getOrElse(v, typ)
        case TApp(tyfun, arg) => TApp(go(tyfun, subst), go(arg, subst))
        case TForall(binder @ (v, _), body) => TForall(binder, go(body, subst - v))
        case TSynApp(tysyn, args) => TSynApp(tysyn, args.map(go(_, subst)))
        case _ => typ
      }

    if (subst.isEmpty) {
      // optimization
      typ
    } else {
      go(typ, subst.toMap)
    }
  }

  // Returns the `pkgIds` and all its dependencies in topological order.
  // A package undefined w.r.t. the function `packages` is treated as a sink.
  @throws[IllegalArgumentException]
  def dependenciesInTopologicalOrder(
      pkgIds: List[Ref.PackageId],
      packages: PartialFunction[Ref.PackageId, Package],
  ): List[Ref.PackageId] = {

    @tailrec
    def buildGraph(
        toProcess0: List[Ref.PackageId],
        seen0: Set[Ref.PackageId],
        graph0: Graphs.Graph[Ref.PackageId],
    ): Graphs.Graph[Ref.PackageId] =
      toProcess0 match {
        case pkgId :: toProcess1 =>
          val deps = packages.lift(pkgId).fold(Set.empty[Ref.PackageId])(_.directDeps)
          val newDeps = deps.filterNot(seen0)
          buildGraph(
            newDeps.foldLeft(toProcess1)(_.::(_)),
            seen0 ++ newDeps,
            graph0.updated(pkgId, deps),
          )
        case Nil => graph0
      }

    Graphs.topoSort(buildGraph(pkgIds, pkgIds.toSet, HashMap.empty)) match {
      case Right(value) =>
        value
      case Left(cycle) =>
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"cycle in package definitions ${cycle.vertices.mkString(" -> ")}",
        )
    }
  }

  private[this] def toSignature(choice: TemplateChoice): TemplateChoiceSignature =
    choice match {
      case TemplateChoice(
            name,
            consuming,
            _,
            choiceObservers,
            choiceAuthorizers,
            selfBinder,
            argBinder,
            returnType,
            _,
          ) =>
        TemplateChoiceSignature(
          name,
          consuming,
          (),
          choiceObservers.map(_ => ()),
          choiceAuthorizers.map(_ => ()),
          selfBinder,
          argBinder,
          returnType,
          (),
        )
    }

  private[this] def toSignature(key: TemplateKey): TemplateKeySignature =
    key match {
      case TemplateKey(typ, _, _) =>
        TemplateKeySignature(typ, (), ())
    }

  private[this] def toSignature(
      iiMethod: InterfaceInstanceMethod
  ): InterfaceInstanceMethodSignature =
    iiMethod match {
      case InterfaceInstanceMethod(name, _) =>
        InterfaceInstanceMethodSignature(name, ())
    }

  private[this] def toSignature(iiBody: InterfaceInstanceBody): InterfaceInstanceBodySignature =
    iiBody match {
      case InterfaceInstanceBody(methods, view @ _) =>
        InterfaceInstanceBodySignature(
          methods.transform((_, v) => toSignature(v)),
          (),
        )
    }

  private[this] def toSignature(implements: TemplateImplements): TemplateImplementsSignature =
    implements match {
      case TemplateImplements(name, body) =>
        TemplateImplementsSignature(
          name,
          toSignature(body),
        )
    }

  private[this] def toSignature(template: Template): TemplateSignature =
    template match {
      case Template(param, _, _, choices, _, key, implements) =>
        TemplateSignature(
          param,
          (),
          (),
          choices.transform((_, v) => toSignature(v)),
          (),
          key.map(toSignature),
          implements.transform((_, v) => toSignature(v)),
        )
    }

  private[this] def toSignature(
      coImplements: InterfaceCoImplements
  ): InterfaceCoImplementsSignature =
    coImplements match {
      case InterfaceCoImplements(name, body) =>
        InterfaceCoImplementsSignature(
          name,
          toSignature(body),
        )
    }

  private def toSignature(interface: DefInterface): DefInterfaceSignature =
    interface match {
      case DefInterface(requires, param, choices, methods, view, coImplements) =>
        DefInterfaceSignature(
          requires,
          param,
          choices.transform((_, choice) => toSignature(choice)),
          methods,
          view,
          coImplements.transform((_, v) => toSignature(v)),
        )
    }

  private[this] def toSignature(module: Module): ModuleSignature =
    module match {
      case Module(name, definitions, templates, exceptions, interfaces, featureFlags) =>
        ModuleSignature(
          name = name,
          definitions = definitions.transform {
            case (_, dvalue: GenDValue[_]) => dvalue.copy(body = ())
            case (_, dataType: DDataType) => dataType
            case (_, typeSyn: DTypeSyn) => typeSyn
          },
          templates = templates.transform((_, template) => toSignature(template)),
          exceptions = exceptions.transform((_, _) => DefExceptionSignature),
          interfaces = interfaces.transform((_, iface) => toSignature(iface)),
          featureFlags = featureFlags,
        )
    }

  def toSignature(p: Package): PackageSignature =
    p.copy(modules = p.modules.transform((_, mod) => toSignature(mod)))

  def toSignatures(pkgs: Map[Ref.PackageId, Package]): Map[Ref.PackageId, PackageSignature] =
    pkgs.transform((_, v) => toSignature(v))

  val NoPackageMetadata = PackageMetadata(
    name = Ref.PackageName.assertFromString("-no-package-metadata"),
    version = Ref.PackageVersion.assertFromString("0"),
    upgradedPackageId = None,
  )

  def collectIdentifiers(mod: Module): Iterable[Ref.Identifier] = {
    import iterable.{ExprIterable, TypeIterable}
    def identifiersInTypes(typ: Type): Iterator[Ref.Identifier] = {
      val ids = typ match {
        case TTyCon(typeConName) => Iterator.single(typeConName)
        case TSynApp(typeSynName, args @ _) => Iterator.single(typeSynName)
        case otherwise @ _ => Iterator.empty
      }
      ids ++ TypeIterable(typ).iterator.flatMap(identifiersInTypes)
    }

    def identifiersInExpr(expr: Expr): Iterator[Ref.Identifier] = {
      val ids = expr match {
        case EVal(valRef) => Iterator.single(valRef)
        case EAbs(binder @ _, body @ _, ref) => ref.iterator
        case otherwise @ _ => Iterator.empty
      }
      ids ++ ExprIterable(expr).flatMap(identifiersInExpr)
    }

    TypeIterable(mod).flatMap(identifiersInTypes) ++
      ExprIterable(mod).flatMap(identifiersInExpr)
  }
}
