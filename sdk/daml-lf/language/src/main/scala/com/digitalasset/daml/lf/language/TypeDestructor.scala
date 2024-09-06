// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.{Numeric, Ref}
import com.digitalasset.daml.lf.language.{Util => AstUtil}

import scala.collection.immutable.ArraySeq

object TypeDestructor {

  sealed abstract class TypeF[+Type] extends Product with Serializable

  object TypeF {

    case object UnitF extends TypeF[Nothing]

    case object BoolF extends TypeF[Nothing]

    case object Int64F extends TypeF[Nothing]

    case object DateF extends TypeF[Nothing]

    case object TimestampF extends TypeF[Nothing]

    final case class NumericF(scale: Numeric.Scale) extends TypeF[Nothing]

    case object PartyF extends TypeF[Nothing]

    case object TextF extends TypeF[Nothing]

    final case class ContractIdF[Type](a: Type) extends TypeF[Type]

    final case class OptionalF[Type](a: Type) extends TypeF[Type]

    final case class ListF[Type](a: Type) extends TypeF[Type]

    final case class MapF[Type](a: Type, b: Type) extends TypeF[Type]

    final case class TextMapF[Type](a: Type) extends TypeF[Type]

    final case class RecordF[Type](
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        fieldNames: ArraySeq[Ref.Name],
        fieldTypes: ArraySeq[Type],
    ) extends TypeF[Type]

    final case class VariantF[Type](
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        cons: ArraySeq[Ref.Name],
        consTypes: ArraySeq[Type],
    ) extends TypeF[Type] {
      private[this] lazy val consRankMap = cons.view.zipWithIndex.toMap

      def consRank(cons: Ref.Name): Either[LookupError, Int] = {
        def ref = Reference.DataEnumConstructor(tyCon, cons)

        consRankMap.get(cons).toRight(LookupError.NotFound(ref, ref))
      }
    }

    final case class EnumF(
        tyCon: Ref.TypeConName,
        pkgName: Ref.PackageName,
        cons: ArraySeq[Ref.Name],
    ) extends TypeF[Nothing] {
      private[this] lazy val consRankMap = cons.view.zipWithIndex.toMap

      def consRank(cons: Ref.Name): Either[LookupError, Int] = {
        def ref = Reference.DataEnumConstructor(tyCon, cons)

        consRankMap.get(cons).toRight(LookupError.NotFound(ref, ref))
      }
    }
  }

  sealed abstract class Error extends Product with Serializable

  object Error {
    final case class LookupError(error: language.LookupError) extends Error

    final case class TypeError(msg: String) extends Error
  }

  def apply(pkgInterface: PackageInterface): TypeDestructor =
    new TypeDestructor(pkgInterface)

}

final class TypeDestructor(pkgInterface: PackageInterface) {
  self =>

  import TypeDestructor.TypeF
  import TypeF._

  private var cache: Map[Ast.Type, TypeF[Ast.Type]] = Map.empty

  def destruct(state: Ast.Type): Either[TypeDestructor.Error, TypeF[Ast.Type]] =
    cache.get(state) match {
      case Some(typeF) =>
        Right(typeF)
      case None =>
        val result = go(state, List.empty)
        val _ = result.foreach(typeF => cache = cache.updated(state, typeF))
        result
    }

  private def go(
      typ0: Ast.Type,
      args: List[Ast.Type],
  ): Either[TypeDestructor.Error, TypeF[Ast.Type]] = {
    def prettyType = args.foldLeft(typ0)(Ast.TApp).pretty

    def unsupportedType = TypeDestructor.Error.TypeError(s"unsupported type $prettyType")

    def wrongType = TypeDestructor.Error.TypeError(s"wrong type $prettyType")

    typ0 match {
      case Ast.TSynApp(tysyn, synArgs) =>
        for {
          synDef <- pkgInterface.lookupTypeSyn(tysyn).left.map(TypeDestructor.Error.LookupError)
          params = synDef.params
          subst <- Either.cond(
            params.length == synArgs.length,
            params.toSeq.view.map(_._1) zip synArgs.toSeq.view,
            TypeDestructor.Error.TypeError(s"wrong number of argument for type synonym $tysyn"),
          )
          typ = AstUtil.substitute(synDef.typ, subst)
          r <- go(typ, args)
        } yield r
      case Ast.TTyCon(tycon) =>
        for {
          pkg <- pkgInterface
            .lookupPackage(tycon.packageId)
            .left
            .map(TypeDestructor.Error.LookupError)
          pkgName = pkg.metadata.name
          dataDef <- pkgInterface.lookupDataType(tycon).left.map(TypeDestructor.Error.LookupError)
          params = dataDef.params
          subst <- Either.cond(
            params.length == args.length,
            params.toSeq.view.map(_._1) zip args.view,
            TypeDestructor.Error.TypeError(s"wrong number of argument for $tycon"),
          )
          destructor <- dataDef.cons match {
            case Ast.DataRecord(fields) =>
              Right(
                RecordF[Ast.Type](
                  tycon,
                  pkgName,
                  fields.toSeq.view.map(_._1).to(Ref.Name.ArraySeq),
                  fields.toSeq.view
                    .map { case (_, typ) => AstUtil.substitute(typ, subst) }
                    .to(ArraySeq),
                )
              )
            case Ast.DataVariant(variants) =>
              Right(
                VariantF[Ast.Type](
                  tycon,
                  pkgName,
                  variants.toSeq.view.map(_._1).to(Ref.Name.ArraySeq),
                  variants.toSeq.view
                    .map { case (_, typ) => AstUtil.substitute(typ, subst) }
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
                Right(NumericF(s))
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
              case typ :: Nil => Right(ContractIdF(typ))
              case _ => Left(wrongType)
            }
          case Ast.BTTextMap =>
            args match {
              case value :: Nil =>
                Right(TextMapF(value))
              case _ => Left(wrongType)
            }
          case Ast.BTGenMap =>
            args match {
              case key :: value :: Nil =>
                Right(MapF(key, value))
              case _ => Left(wrongType)
            }
          case Ast.BTList =>
            args match {
              case typ :: Nil => Right(ListF(typ))
              case _ => Left(wrongType)
            }
          case Ast.BTOptional =>
            args match {
              case typ :: Nil => Right(OptionalF(typ))
              case _ => Left(wrongType)
            }
          case _ =>
            Left(unsupportedType)
        }
      case Ast.TApp(tyfun, arg) =>
        go(tyfun, arg :: args)
      case _ =>
        Left(unsupportedType)
    }
  }
}
