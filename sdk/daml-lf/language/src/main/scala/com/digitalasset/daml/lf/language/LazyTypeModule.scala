// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Util => AstUtil}

import scala.collection.immutable.ArraySeq

sealed abstract class LazyTypeModule {
  sealed abstract class LazyType {
    def typ: Ast.Type
    def destruct: Either[LazyTypeModule.Error, TypeF[LazyType]]
  }

  def toLazy(typ: Ast.Type): LazyType

  def apply(typ: Ast.Type): LazyType = toLazy(typ)
}

object LazyTypeModule {

  def apply(pkgInterface: PackageInterface): LazyTypeModule =
    new LazyTypeModuleImpl(pkgInterface)

  sealed abstract class Error extends Product with Serializable
  object Error {
    final case class LookupError(error: language.LookupError) extends Error
    final case class TypeError(msg: String) extends Error
  }

}

private final class LazyTypeModuleImpl(pkgInterface: PackageInterface) extends LazyTypeModule {
  self =>

  import TypeF._
  import LazyTypeModule._

  def toLazy(typ: Ast.Type): LazyTypeImpl =
    cache.get(typ) match {
      case Some(lazyType) => lazyType
      case None =>
        val lazyType = new LazyTypeImpl(typ)
        cache = cache.updated(typ, lazyType)
        lazyType
    }

  private def destruct(
      typ0: Ast.Type,
      args: List[Ast.Type] = List.empty,
  ): Either[Error, TypeF[LazyTypeImpl]] = {
    def prettyType = args.foldLeft(typ0)(Ast.TApp).pretty

    def unsupportedType = Error.TypeError(s"unsupported type $prettyType")

    def wrongType = Error.TypeError(s"wrong type $prettyType")

    typ0 match {
      case Ast.TSynApp(tysyn, synArgs) =>
        for {
          synDef <- pkgInterface.lookupTypeSyn(tysyn).left.map(Error.LookupError)
          params = synDef.params
          subst <- Either.cond(
            params.length == synArgs.length,
            params.toSeq.view.map(_._1) zip synArgs.toSeq.view,
            Error.TypeError(s"wrong number of argument for type synonyme $tysyn"),
          )
          typ = AstUtil.substitute(synDef.typ, subst)
          r <- destruct(typ, args)
        } yield r
      case Ast.TTyCon(tycon) =>
        for {
          pkg <- pkgInterface.lookupPackage(tycon.packageId).left.map(Error.LookupError)
          pkgName = pkg.metadata.name
          dataDef <- pkgInterface.lookupDataType(tycon).left.map(Error.LookupError)
          params = dataDef.params
          subst <- Either.cond(
            params.length == args.length,
            params.toSeq.view.map(_._1) zip args.view,
            Error.TypeError(s"wrong number of argument for $tycon"),
          )
          destructor <- dataDef.cons match {
            case Ast.DataRecord(fields) =>
              Right(
                RecordF[LazyTypeImpl](
                  tycon,
                  pkgName,
                  fields.toSeq.view.map(_._1).to(Ref.Name.ArraySeq),
                  fields.toSeq.view
                    .map { case (_, typ) => toLazy(AstUtil.substitute(typ, subst)) }
                    .to(ArraySeq),
                )
              )
            case Ast.DataVariant(variants) =>
              Right(
                VariantF[LazyTypeImpl](
                  tycon,
                  pkgName,
                  variants.toSeq.view.map(_._1).to(Ref.Name.ArraySeq),
                  variants.toSeq.view
                    .map { case (_, typ) => toLazy(AstUtil.substitute(typ, subst)) }
                    .to(ArraySeq),
                )
              )
            case Ast.DataEnum(constructors) =>
              Right(
                EnumF(
                  tycon,
                  pkgName,
                  constructors.toSeq.to(Ref.Name.ArraySeq),
                )
              )
            case Ast.DataInterface =>
              Left(unsupportedType)
          }
        } yield destructor
      case Ast.TBuiltin(bt) =>
        bt match {
          case Ast.BTInt64 =>
            Either.cond(args.isEmpty, Int64F, wrongType)
          case Ast.BTNumeric =>
            args match {
              case Ast.TNat(s) :: Nil =>
                Right(TypeF.NumericF(s))
              case _ =>
                Left(wrongType)
            }
          case Ast.BTText =>
            Either.cond(args.isEmpty, TextF, wrongType)
          case Ast.BTTimestamp =>
            Either.cond(args.isEmpty, TimestampF, wrongType)
          case Ast.BTParty =>
            Either.cond(args.isEmpty, PartyF, wrongType)
          case Ast.BTUnit =>
            Either.cond(args.isEmpty, UnitF, wrongType)
          case Ast.BTBool =>
            Either.cond(args.isEmpty, BoolF, wrongType)
          case Ast.BTDate =>
            Either.cond(args.isEmpty, DateF, wrongType)
          case Ast.BTContractId =>
            args match {
              case typ :: Nil => Right(ContractIdF(toLazy(typ)))
              case _ => Left(wrongType)
            }
          case Ast.BTTextMap =>
            args match {
              case value :: Nil =>
                Right(TextMapF(toLazy(value)))
              case _ => Left(wrongType)
            }
          case Ast.BTGenMap =>
            args match {
              case key :: value :: Nil =>
                Right(MapF(toLazy(key), toLazy(value)))
              case _ => Left(wrongType)
            }
          case Ast.BTList =>
            args match {
              case typ :: Nil => Right(ListF(toLazy(typ)))
              case _ => Left(wrongType)
            }
          case Ast.BTOptional =>
            args match {
              case typ :: Nil => Right(OptionalF(toLazy(typ)))
              case _ => Left(wrongType)
            }
          case _ =>
            Left(unsupportedType)
        }
      case Ast.TApp(tyfun, arg) =>
        destruct(tyfun, arg :: args)
      case _ =>
        Left(unsupportedType)
    }
  }

  private var cache: Map[Ast.Type, LazyTypeImpl] = Map.empty

  final class LazyTypeImpl(override val typ: Ast.Type) extends LazyType {
    override def toString: String = s"LazyType(${typ.pretty})"
    def destruct: Either[Error, TypeF[LazyType]] = self.destruct(typ)

  }
}
