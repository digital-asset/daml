// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2.damldefinitionsservice

import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.*
import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.daml.lf.language.TypeSig.*
import com.digitalasset.daml.lf.language.{Ast, LookupError, PackageInterface}

import scala.annotation.tailrec
import scala.collection.immutable.VectorMap
import scala.collection.mutable

object DamlDefinitionsBuilders {
  def buildTypeSig(
      pkgId: Ref.PackageId,
      pkg: Ast.PackageSignature,
      allPackages: Map[Ref.PackageId, Ast.PackageSignature],
  ): TypeSig =
    toTypeSig(pkgId, pkg, new PackageInterface(allPackages))

  def buildTemplateDefinition(
      templateId: Ref.Identifier,
      allPackages: Map[Ref.PackageId, Ast.PackageSignature],
  ): Option[TemplateDefinition] = {
    val packageInterface = new PackageInterface(allPackages)

    // TODO(#21695): Do not rebuild globalTypeSig on each query
    val globalTypeSig = allPackages.foldLeft(Schema.Empty) { case (acc, (pkgId, pkg)) =>
      acc.merge(toTypeSig(pkgId, pkg, packageInterface))
    }

    globalTypeSig.templateDefs.get(templateId).map { templateSig =>
      val (choices, dependencyDefs) = choicesDefinitions(templateSig.choices, globalTypeSig)

      val definitionsBuilder = Map.newBuilder[Ref.TypeConName, DataTypeSig]
      definitionsBuilder ++= dependencyDefs
      TemplateDefinition(
        arguments = {
          // TODO(#21695): Use internal error
          val argRecordDef = globalTypeSig.recordDefs(templateId)
          argRecordDef.fields.view
            .flatMap { case (_fieldName, serializableType) =>
              extractFirstLevelTypeCons(serializableType).toList
            }
            .foreach { typeCons =>
              definitionsBuilder ++= gatherTypeDefs(
                mutable.Queue(typeCons),
                Map.empty,
                globalTypeSig,
              )
            }
          argRecordDef
        },
        key = templateSig.key,
        choices = choices,
        implements = templateSig.implements.view.map { interfaceId =>
          // TODO(#21695): Use internal error
          val interfaceSig = globalTypeSig.interfaceDefs(interfaceId)
          interfaceId -> InterfaceDefinition(
            choices = {
              val (choices, dependencyDefs) =
                choicesDefinitions(interfaceSig.choices, globalTypeSig)
              definitionsBuilder ++= dependencyDefs
              choices
            },
            viewType = {
              // Interface views types are always of a records
              val tyCon = interfaceSig.viewType.asInstanceOf[SerializableType.Record].tyCon
              definitionsBuilder ++= gatherTypeDefs(
                mutable.Queue(tyCon),
                Map.empty,
                globalTypeSig,
              )
              globalTypeSig.recordDefs(tyCon)
            },
          )
        }.toMap,
        definitions = definitionsBuilder.result(),
      )
    }
  }

  private def toTypeSig(
      pkgId: Ref.PackageId,
      pkg: PackageSignature,
      packageInterface: PackageInterface,
  ) = {
    // TODO(#21695): Convert to tailrec
    def toSerializableType(typ: Ast.Type, args: List[Ast.Type]): SerializableType = {
      def nonSerializable = nonSerializableType(args.foldLeft(typ)(Ast.TApp.apply))

      typ match {
        case Ast.TVar(name) => SerializableType.Var(name)
        case Ast.TTyCon(tycon) =>
          val dataType = handleLookup(packageInterface.lookupDataType(tycon))
          dataType.cons match {
            case Ast.DataRecord(_) =>
              SerializableType.Record(
                tycon,
                args.map(toSerializableType(_, List.empty)),
              )
            case Ast.DataVariant(_) =>
              SerializableType.Variant(
                tycon,
                args.map(toSerializableType(_, List.empty)),
              )
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
                case _ => nonSerializable
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
                      packageInterface.lookupTemplateOrInterface(tycon) match {
                        case Right(data.TemplateOrInterface.Template(_)) =>
                          Some(TemplateOrInterface.Template(tycon))
                        case Right(data.TemplateOrInterface.Interface(_)) =>
                          Some(TemplateOrInterface.Interface(tycon))
                        case Left(_) =>
                          None
                      }
                    case _x =>
                      None
                  }
                  SerializableType.ContractId(typeId)
                case _ => nonSerializable
              }
            case k :: v :: Nil =>
              bt match {
                case Ast.BTGenMap =>
                  SerializableType.GenMap(
                    toSerializableType(k, List.empty),
                    toSerializableType(v, List.empty),
                  )
                case _ => nonSerializable
              }
            case _ => nonSerializable
          }
        case Ast.TApp(tyfun, arg) =>
          toSerializableType(tyfun, arg :: args)
        case _ => nonSerializable
      }
    }

    def toChoiceSig(choiceDef: Ast.GenTemplateChoice[_]): ChoiceSig =
      ChoiceSig(
        consuming = choiceDef.consuming,
        argType = toSerializableType(choiceDef.argBinder._2, List.empty),
        returnType = toSerializableType(choiceDef.returnType, List.empty),
      )

    pkg.modules.iterator.foldLeft(Schema.Empty) { case (acc0, (modName, mod)) =>
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

  private def handleLookup[X](errorOrX: Either[LookupError, X]) =
    errorOrX match {
      case Right(value) => value
      case Left(err) => throw new Error(err.pretty)
    }

  private def nonSerializableType(typ: Ast.Type): Nothing =
    throw new Error(s"unexpected non serializable type ${typ.pretty}")

  private def choicesDefinitions(
      choiceSigs: Map[Ref.Name, ChoiceSig],
      globalTypeSig: TypeSig,
  ): (Map[Ref.Name, ChoiceDefinition], Map[Ref.TypeConName, DataTypeSig]) = {
    val definitionsMapBuilder = Map.newBuilder[Ref.TypeConName, DataTypeSig]
    val choices = choiceSigs.map { case (choiceName, choiceSig) =>
      choiceName -> ChoiceDefinition(
        consuming = choiceSig.consuming,
        arguments = {
          val choiceRecordTyCon =
            // Choice arguments are always records
            choiceSig.argType.asInstanceOf[SerializableType.Record].tyCon
          val choiceRecordSig = globalTypeSig.recordDefs(choiceRecordTyCon)
          definitionsMapBuilder ++= gatherTypeDefs(
            mutable.Queue(choiceRecordTyCon),
            Map.empty,
            globalTypeSig,
          )
          choiceRecordSig
        },
        returnType = {
          extractFirstLevelTypeCons(choiceSig.returnType).foreach(typeCons =>
            definitionsMapBuilder ++= gatherTypeDefs(
              mutable.Queue(typeCons),
              Map.empty,
              globalTypeSig,
            )
          )
          choiceSig.returnType
        },
      )
    }
    choices -> definitionsMapBuilder.result()
  }

  private def extractFirstLevelTypeCons(
      serializableType: SerializableType
  ): Option[Ref.TypeConName] =
    serializableType match {
      case SerializableType.Enum(tycon) => Some(tycon)
      case SerializableType.Record(tyCon, _) => Some(tyCon)
      case SerializableType.Variant(tyCon, _) => Some(tyCon)
      case SerializableType.ContractId(typeId) =>
        typeId.flatMap {
          case TemplateOrInterface.Template(tycon) => Some(tycon)
          case TemplateOrInterface.Interface(tycon) => Some(tycon)
        }
      case SerializableType.List(typeParam) => extractFirstLevelTypeCons(typeParam)
      case SerializableType.Optional(typeParam) => extractFirstLevelTypeCons(typeParam)
      case SerializableType.GenMap(k, v) =>
        extractFirstLevelTypeCons(k).orElse(extractFirstLevelTypeCons(v))
      case _ => None
    }

  @tailrec
  private def gatherTypeDefs(
      typeCons: mutable.Queue[Ref.TypeConName],
      acc: Map[Ref.TypeConName, DataTypeSig],
      globalTypeSig: TypeSig,
  ): Map[Ref.TypeConName, DataTypeSig] =
    if (typeCons.isEmpty) acc
    else {
      val typeCon = typeCons.dequeue()
      if (acc.contains(typeCon)) gatherTypeDefs(typeCons, acc, globalTypeSig)
      else {
        val dataTypeO = globalTypeSig.recordDefs
          .get(typeCon)
          .orElse(globalTypeSig.enumDefs.get(typeCon))
          .orElse(globalTypeSig.variantDefs.get(typeCon))

        val updatedMap = dataTypeO.map(acc.updated(typeCon, _)).getOrElse(acc)

        // Avoid Option#map to allow tailrec
        dataTypeO match {
          case Some(RecordSig(_, fields)) =>
            gatherTypeDefs(
              typeCons.enqueueAll(fields.values.flatMap(extractFirstLevelTypeCons(_).toList)),
              updatedMap,
              globalTypeSig,
            )
          case Some(VariantSig(_, constructor)) =>
            gatherTypeDefs(
              typeCons.enqueueAll(
                constructor.values.flatMap(extractFirstLevelTypeCons(_).toList)
              ),
              updatedMap,
              globalTypeSig,
            )
          case Some(EnumSig(_)) |
              // We assume that the type does not have an associated definition (i.e. it is an interface)
              None =>
            // Recurse to consume the entire queue
            gatherTypeDefs(typeCons, updatedMap, globalTypeSig)
        }
      }
    }
}
