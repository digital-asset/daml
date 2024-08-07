// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.Ref
import TypeSig._

final case class TypeSig(
    enumDefs: Map[Ref.TypeConName, EnumSig],
    variantDefs: Map[Ref.TypeConName, VariantSig],
    recordDefs: Map[Ref.TypeConName, RecordSig],
    templateDefs: Map[Ref.TypeConName, TemplateSig],
    interfaceDefs: Map[Ref.TypeConName, InterfaceSig],
) {

  def addenumDefs(
      pkgId: Ref.PackageId,
      modName: Ref.ModuleName,
      name: Ref.DottedName,
      defn: EnumSig,
  ) = {
    val tyConName = Ref.Identifier(pkgId, Ref.QualifiedName(modName, name))
    copy(enumDefs = enumDefs.updated(tyConName, defn))
  }
  def addvariantDefs(
      pkgId: Ref.PackageId,
      modName: Ref.ModuleName,
      name: Ref.DottedName,
      defn: VariantSig,
  ) = {
    val tyConName = Ref.Identifier(pkgId, Ref.QualifiedName(modName, name))
    copy(variantDefs = variantDefs.updated(tyConName, defn))

  }
  def addrecordDefs(
      pkgId: Ref.PackageId,
      modName: Ref.ModuleName,
      name: Ref.DottedName,
      defn: RecordSig,
  ) = {
    val tyConName = Ref.Identifier(pkgId, Ref.QualifiedName(modName, name))
    copy(recordDefs = recordDefs.updated(tyConName, defn))

  }
  def addtemplateDefs(
      pkgId: Ref.PackageId,
      modName: Ref.ModuleName,
      name: Ref.DottedName,
      defn: TemplateSig,
  ) = {
    val tyConName = Ref.Identifier(pkgId, Ref.QualifiedName(modName, name))
    copy(templateDefs = templateDefs.updated(tyConName, defn))

  }
  def addinterfaceDefs(
      pkgId: Ref.PackageId,
      modName: Ref.ModuleName,
      name: Ref.DottedName,
      defn: InterfaceSig,
  ) = {
    val tyConName = Ref.Identifier(pkgId, Ref.QualifiedName(modName, name))
    copy(interfaceDefs = interfaceDefs.updated(tyConName, defn))
  }

}

object TypeSig {

  val Empty = TypeSig(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)

  sealed abstract class SerializableType extends Product with Serializable

  sealed abstract class TemplateOrInterface extends Product with Serializable

  object TemplateOrInterface {
    final case class Template(tycon: Ref.TypeConName) extends TemplateOrInterface

    final case class Interface(tycon: Ref.TypeConName) extends TemplateOrInterface
  }

  object SerializableType {
    final case class Enum(tycon: Ref.TypeConName) extends SerializableType

    case object Int64 extends SerializableType

    final case class Numeric(scale: data.Numeric.Scale) extends SerializableType

    case object Text extends SerializableType

    case object Timestamp extends SerializableType

    case object Date extends SerializableType

    case object Party extends SerializableType

    case object Bool extends SerializableType

    case object Unit extends SerializableType

    final case class Record(tyCon: Ref.TypeConName, params: Seq[SerializableType])
        extends SerializableType

    final case class Variant(tyCon: Ref.TypeConName, params: Seq[SerializableType])
        extends SerializableType

    final case class ContractId(typeId: Option[TemplateOrInterface]) extends SerializableType

    final case class List(typ: SerializableType) extends SerializableType

    final case class Optional(typ: SerializableType) extends SerializableType

    final case class GenMap(k: SerializableType, v: SerializableType) extends SerializableType

    final case class Var(name: Ref.Name) extends SerializableType
  }

  final case class RecordSig(params: Seq[Ref.Name], fields: Seq[(Ref.Name, SerializableType)])

  final case class VariantSig(params: Seq[Ref.Name], constructor: Seq[(Ref.Name, SerializableType)])

  final case class EnumSig(constructor: Seq[Ref.Name])

  final case class ChoiceSig(
      consuming: Boolean,
      argType: SerializableType,
      returnType: SerializableType,
  )

  final case class TemplateSig(
      key: Option[SerializableType],
      choices: Map[Ref.Name, ChoiceSig],
      implements: Set[Ref.TypeConName],
  )

  final case class InterfaceSig(choices: Map[Ref.Name, ChoiceSig], viewType: SerializableType)

  def handleLookup[X](errorOrX: Either[LookupError, X]) =
    errorOrX match {
      case Right(value) => value
      case Left(err) => throw new Error(err.pretty)
    }

  def nonSeriliazableType(typ: Ast.Type): Nothing =
    throw new Error(s"unexpected non serializable type ${typ.pretty}")

  def fromPackage(
      pkgId: Ref.PackageId,
      pkg: Ast.Package,
      pkgInterface: PackageInterface,
  ): TypeSig = {

    def toSerializableType(typ: Ast.Type, args: List[Ast.Type]): SerializableType = {
      def nonSerializable =
        nonSeriliazableType(args.foldLeft(typ)(Ast.TApp))

      typ match {
        case Ast.TVar(name) => SerializableType.Var(name)
        case Ast.TTyCon(tycon) =>
          val dataType = handleLookup(pkgInterface.lookupDataType(tycon))
          dataType.cons match {
            case Ast.DataRecord(_) =>
              SerializableType.Record(tycon, args.map(toSerializableType(_, List.empty)))
            case Ast.DataVariant(_) =>
              SerializableType.Variant(tycon, args.map(toSerializableType(_, List.empty)))
            case Ast.DataEnum(_) =>
              SerializableType.Enum(tycon)
            case Ast.DataInterface =>
              nonSerializable
          }
        case Ast.TBuiltin(bt) =>
          args match {
            case Nil =>
              bt match {
                case Ast.BTInt64 => SerializableType.Int64
                case Ast.BTText => SerializableType.Text
                case Ast.BTTimestamp => SerializableType.Timestamp
                case Ast.BTParty => SerializableType.Party
                case Ast.BTUnit => SerializableType.Unit
                case Ast.BTBool => SerializableType.Bool
                case _ =>
                  nonSerializable
              }
            case arg :: Nil =>
              bt match {
                case Ast.BTNumeric =>
                  arg match {
                    case Ast.TNat(n) => SerializableType.Numeric(n)
                    case _ => nonSerializable
                  }
                case Ast.BTList =>
                  SerializableType.List(toSerializableType(arg, List.empty))
                case Ast.BTOptional =>
                  SerializableType.Optional(toSerializableType(arg, List.empty))
                case Ast.BTContractId =>
                  val typeId = arg match {
                    case Ast.TTyCon(tycon) if args.isEmpty =>
                      pkgInterface.lookupTemplateOrInterface(tycon) match {
                        case Right(data.TemplateOrInterface.Template(_)) =>
                          Some(TemplateOrInterface.Template(tycon))
                        case Right(data.TemplateOrInterface.Interface(_)) =>
                          Some(TemplateOrInterface.Interface(tycon))
                        case Left(_) => None
                      }
                    case _ => None
                  }
                  SerializableType.ContractId(typeId)
                case _ =>
                  nonSerializable
              }
            case k :: v :: Nil =>
              bt match {
                case Ast.BTGenMap =>
                  SerializableType.GenMap(
                    toSerializableType(k, List.empty),
                    toSerializableType(v, List.empty),
                  )
                case _ =>
                  nonSerializable
              }
            case _ =>
              nonSerializable
          }
        case Ast.TApp(tyfun, arg) =>
          toSerializableType(tyfun, arg :: args)
        case _ =>
          nonSerializable
      }
    }

    def toChoiceSig(choiceDef: Ast.GenTemplateChoice[_]): ChoiceSig =
      ChoiceSig(
        consuming = choiceDef.consuming,
        argType = toSerializableType(choiceDef.argBinder._2, List.empty),
        returnType = toSerializableType(choiceDef.returnType, List.empty),
      )

    pkg.modules.iterator.foldLeft(Empty) { case (acc0, (modName, mod)) =>
      val acc1 = mod.definitions.foldLeft(acc0) { case (acc, (name, defn)) =>
        defn match {
          case Ast.DDataType(true, params, cons) =>
            cons match {
              case Ast.DataRecord(fields) =>
                acc.addrecordDefs(
                  pkgId,
                  modName,
                  name,
                  RecordSig(
                    params.iterator.map(_._1).toSeq,
                    fields.iterator.map { case (name, typ) =>
                      name -> toSerializableType(typ, List.empty)
                    }.toSeq,
                  ),
                )
              case Ast.DataVariant(variants) =>
                acc.addvariantDefs(
                  pkgId,
                  modName,
                  name,
                  VariantSig(
                    params.iterator.map(_._1).toSeq,
                    variants.iterator.map { case (name, typ) =>
                      name -> toSerializableType(typ, List.empty)
                    }.toSeq,
                  ),
                )
              case Ast.DataEnum(constructors) =>
                acc.addenumDefs(pkgId, modName, name, EnumSig(constructors.toSeq))
              case Ast.DataInterface =>
                acc
            }
          case _ =>
            acc
        }
      }
      val acc2 = mod.templates.foldLeft(acc1) {
        case (acc, (name, Ast.GenTemplate(_, _, _, choices, _, key, implements))) =>
          acc.addtemplateDefs(
            pkgId,
            modName,
            name,
            TemplateSig(
              key.map(defn => toSerializableType(defn.typ, List.empty)),
              choices.transform((_, v) => toChoiceSig(v)),
              implements.keySet,
            ),
          )
      }
      val acc3 = mod.interfaces.foldLeft(acc2) {
        case (acc, (name, Ast.GenDefInterface(_, _, choices, _, _, viewType))) =>
          acc.addinterfaceDefs(
            pkgId,
            modName,
            name,
            InterfaceSig(
              choices.transform((_, v) => toChoiceSig(v)),
              toSerializableType(viewType, List.empty),
            ),
          )
      }
      acc3
    }
  }
}
