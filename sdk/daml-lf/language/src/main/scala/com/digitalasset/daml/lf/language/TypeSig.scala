// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.daml.lf.language.TypeSig._

import scala.collection.immutable.VectorMap

final case class TypeSig(
    enumDefs: Map[Ref.TypeConName, EnumSig],
    variantDefs: Map[Ref.TypeConName, VariantSig],
    recordDefs: Map[Ref.TypeConName, RecordSig],
    templateDefs: Map[Ref.TypeConName, TemplateSig],
    interfaceDefs: Map[Ref.TypeConName, InterfaceSig],
) {

  def addEnumDefs(id: Ref.TypeConName, defn: EnumSig): TypeSig =
    copy(enumDefs = enumDefs.updated(id, defn))

  def addVariantDefs(id: Ref.TypeConName, defn: VariantSig): TypeSig =
    copy(variantDefs = variantDefs.updated(id, defn))

  def addRecordDefs(id: Ref.TypeConName, defn: RecordSig): TypeSig =
    copy(recordDefs = recordDefs.updated(id, defn))

  def addTemplateDefs(id: Ref.TypeConName, defn: TemplateSig): TypeSig =
    copy(templateDefs = templateDefs.updated(id, defn))

  def addInterfaceDefs(id: Ref.TypeConName, defn: InterfaceSig): TypeSig =
    copy(interfaceDefs = interfaceDefs.updated(id, defn))

  def merge(other: TypeSig): TypeSig = TypeSig(
    enumDefs ++ other.enumDefs,
    variantDefs ++ other.variantDefs,
    recordDefs ++ other.recordDefs,
    templateDefs ++ other.templateDefs,
    interfaceDefs ++ other.interfaceDefs,
  )
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

  sealed trait DataTypeSig extends Product with Serializable

  final case class RecordSig(params: Seq[Ref.Name], fields: VectorMap[Ref.Name, SerializableType])
      extends DataTypeSig

  final case class VariantSig(
      params: Seq[Ref.Name],
      constructor: VectorMap[Ref.Name, SerializableType],
  ) extends DataTypeSig

  final case class EnumSig(constructor: Seq[Ref.Name]) extends DataTypeSig

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

  def nonSerializableType(typ: Ast.Type): Nothing =
    throw new Error(s"unexpected non serializable type ${typ.pretty}")

  def fromPackage(
      pkgId: Ref.PackageId,
      pkg: PackageSignature,
      pkgInterface: PackageInterface,
  ) = {

    def toSerializableType(typ: Ast.Type, args: List[Ast.Type]): SerializableType = {
      def nonSerializable = nonSerializableType(args.foldLeft(typ)(Ast.TApp))

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
                case Ast.BTDate => SerializableType.Date
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
                    case Ast.TTyCon(tycon) =>
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
                acc.addRecordDefs(
                  Ref.Identifier(pkgId, Ref.QualifiedName(modName, name)),
                  RecordSig(
                    params.iterator.map(_._1).toSeq,
                    VectorMap.from(fields.iterator.map { case (name, typ) =>
                      name -> toSerializableType(typ, List.empty)
                    }),
                  ),
                )
              case Ast.DataVariant(variants) =>
                acc.addVariantDefs(
                  Ref.Identifier(pkgId, Ref.QualifiedName(modName, name)),
                  VariantSig(
                    params.iterator.map(_._1).toSeq,
                    VectorMap.from(variants.iterator.map { case (name, typ) =>
                      name -> toSerializableType(typ, List.empty)
                    }),
                  ),
                )
              case Ast.DataEnum(constructors) =>
                acc.addEnumDefs(
                  Ref.Identifier(pkgId, Ref.QualifiedName(modName, name)),
                  EnumSig(constructors.toSeq),
                )
              case Ast.DataInterface =>
                acc
            }
          case _ =>
            acc
        }
      }
      val acc2 = mod.templates.foldLeft(acc1) {
        case (acc, (name, Ast.GenTemplate(_, _, _, choices, _, key, implements))) =>
          acc.addTemplateDefs(
            Ref.Identifier(pkgId, Ref.QualifiedName(modName, name)),
            TemplateSig(
              key.map(defn => toSerializableType(defn.typ, List.empty)),
              choices.view.mapValues(toChoiceSig).toMap,
              implements.keySet,
            ),
          )
      }
      mod.interfaces.foldLeft(acc2) {
        case (acc, (name, Ast.GenDefInterface(_, _, choices, _, _, viewType))) =>
          acc.addInterfaceDefs(
            Ref.Identifier(pkgId, Ref.QualifiedName(modName, name)),
            InterfaceSig(
              choices.view.mapValues(toChoiceSig).toMap,
              toSerializableType(viewType, List.empty),
            ),
          )
      }
    }
  }
}
