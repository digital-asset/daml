// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package typesig
package reader

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.ArchivePayload
import scalaz.{Enum => _, _}
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.std.map._
import scalaz.std.option._
import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.language.Ast
import com.daml.lf.language.{Util => AstUtil}
import com.daml.nonempty.NonEmpty

import scala.collection.immutable.Map

object SignatureReader {
  import Errors._
  import PackageSignature.TypeDecl

  sealed abstract class Error extends Product with Serializable
  final case class UnserializableDataType(error: String) extends Error
  final case class InvalidDataTypeDefinition(error: String) extends Error

  private def errorMessage(ctx: QualifiedName, reason: String): String =
    s"Invalid data definition: $ctx, reason: $reason"

  private def invalidDataTypeDefinition[Bot](
      ctx: QualifiedName,
      reason: String,
  ): Error \/ Bot = -\/(InvalidDataTypeDefinition(errorMessage(ctx, reason)))

  private def unserializableDataType[Bot](
      ctx: QualifiedName,
      reason: String,
  ): Error \/ Bot = -\/(UnserializableDataType(errorMessage(ctx, reason)))

  object Error {
    type Tree = Errors[ErrorLoc, Error]

    implicit def `IRE semigroup`: Semigroup[Error] =
      Semigroup.firstSemigroup

    def treeReport(errors: Errors[ErrorLoc, SignatureReader.InvalidDataTypeDefinition]): Cord =
      stringReport(errors)(
        _.fold(prop => Cord(s".${prop.name}"), ixName => Cord(s"'$ixName'")),
        e => Cord(e.error),
      )
  }

  private[reader] final case class State(
      typeDecls: Map[QualifiedName, TypeDecl] = Map.empty,
      interfaces: Map[QualifiedName, typesig.DefInterface.FWT] = Map.empty,
      errors: Error.Tree = mzero[Error.Tree],
  ) {
    def asOut(packageId: PackageId, metadata: Option[PackageMetadata]): typesig.PackageSignature =
      typesig.PackageSignature(packageId, metadata, typeDecls, interfaces)
  }

  private[reader] object State {
    implicit val stateMonoid: Monoid[State] =
      Monoid.instance(
        (l, r) =>
          State(
            l.typeDecls ++ r.typeDecls,
            l.interfaces ++ r.interfaces,
            l.errors |+| r.errors,
          ),
        State(),
      )
  }

  def readPackageSignature(
      lf: DamlLf.Archive
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], typesig.PackageSignature) =
    readPackageSignature(() => DamlLfArchiveReader.readPackage(lf))

  def readPackageSignature(
      packageId: Ref.PackageId,
      damlLf: DamlLf.ArchivePayload,
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], typesig.PackageSignature) =
    readPackageSignature(() => DamlLfArchiveReader.readPackage(packageId, damlLf))

  def readPackageSignature(
      payload: ArchivePayload
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], typesig.PackageSignature) =
    readPackageSignature(() => DamlLfArchiveReader.readPackage(payload))

  private val dummyPkgId = PackageId.assertFromString("-dummyPkg-")

  private val dummyInterface = typesig.PackageSignature(dummyPkgId, None, Map.empty, Map.empty)

  def readPackageSignature(
      f: () => String \/ (PackageId, Ast.Package)
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], typesig.PackageSignature) =
    f() match {
      case -\/(e) =>
        (point(InvalidDataTypeDefinition(e)), dummyInterface)
      case \/-((templateGroupId, lfPackage)) =>
        lfprintln(s"templateGroupId: $templateGroupId")
        lfprintln(s"package: $lfPackage")
        val s: State = {
          import scalaz.std.iterable._
          lfPackage.modules.values.foldMap(foldModule)
        }
        val r = (
          filterOutUnserializableErrors(s.errors),
          s.asOut(templateGroupId, lfPackage.metadata.map(toIfaceMetadata(_))),
        )
        lfprintln(s"result: $r")
        r
    }

  private def toIfaceMetadata(metadata: Ast.PackageMetadata): PackageMetadata =
    PackageMetadata(metadata.name, metadata.version)

  private def filterOutUnserializableErrors(
      es: Error.Tree
  ): Errors[ErrorLoc, InvalidDataTypeDefinition] =
    es.collectAndPrune { case x: InvalidDataTypeDefinition => x }

  private[reader] def foldModule(module: Ast.Module): State = {
    val (derrors, dataTypes) = (module.definitions: Iterable[(Ref.DottedName, Ast.Definition)])
      .collect { case (name, Ast.DDataType(true, params, dataType)) =>
        val fullName = QualifiedName(module.name, name)
        val tyVars: ImmArraySeq[Ast.TypeVarName] = params.map(_._1).toSeq

        val result: Error \/ Option[(QualifiedName, TypeDecl)] =
          dataType match {
            case dfn: Ast.DataRecord =>
              val it = module.templates.get(name) match {
                case Some(tmpl) => template(fullName, dfn, tmpl)
                case None => record(fullName, tyVars, dfn)
              }
              it map some
            case dfn: Ast.DataVariant =>
              variant(fullName, tyVars, dfn) map some
            case dfn: Ast.DataEnum =>
              enumeration(fullName, tyVars, dfn) map some
            case Ast.DataInterface =>
              // ^ never actually used, as far as I can tell -SC
              \/-(none)
          }
        locate(Symbol("name"), rootErrOf[ErrorLoc](result)).toEither
      }
      .partitionMap(identity)
    val ddts = dataTypes.view.collect { case Some(x) => x }.toMap

    val (ierrors, astIfs) = module.interfaces.partitionMap { case (name, astIf) =>
      val fullName = QualifiedName(module.name, name)
      val result = interface(fullName, astIf)
      locate(Symbol("name"), rootErrOf[ErrorLoc](result)).toEither
    }

    import scalaz.std.iterable._
    State(typeDecls = ddts, interfaces = astIfs.toMap, errors = (derrors ++ ierrors).suml)
  }

  private[reader] def record[T >: TypeDecl.Normal](
      name: QualifiedName,
      tyVars: ImmArraySeq[Ast.TypeVarName],
      record: Ast.DataRecord,
  ) =
    for {
      fields <- fieldsOrCons(name, record.fields)
    } yield name -> (TypeDecl.Normal(DefDataType(tyVars, Record(fields))): T)

  private[reader] def template[T >: TypeDecl.Template](
      name: QualifiedName,
      record: Ast.DataRecord,
      dfn: Ast.Template,
  ) =
    for {
      fields <- fieldsOrCons(name, record.fields)
      choices <- dfn.choices traverse (visitChoice(name, _))
      key <- dfn.key traverse (k => toIfaceType(name, k.typ))
    } yield name -> (TypeDecl.Template(
      Record(fields),
      DefTemplate(visitChoices(choices, dfn.implements), key, dfn.implements.keys),
    ): T)

  private[this] def visitChoices[Ty](
      choices: Map[Ref.ChoiceName, TemplateChoice[Ty]],
      astInterfaces: Map[Ref.TypeConName, Ast.GenTemplateImplements[_]],
  ): TemplateChoices[Ty] =
    astInterfaces.keySet match {
      case NonEmpty(unresolvedInherited) =>
        TemplateChoices.Unresolved(choices, unresolvedInherited)
      case _ => TemplateChoices.Resolved fromDirect choices
    }

  private def visitChoice(
      ctx: QualifiedName,
      choice: Ast.TemplateChoice,
  ): Error \/ TemplateChoice[Type] =
    for {
      tParam <- toIfaceType(ctx, choice.argBinder._2)
      tReturn <- toIfaceType(ctx, choice.returnType)
    } yield TemplateChoice(
      param = tParam,
      consuming = choice.consuming,
      returnType = tReturn,
    )

  private[reader] def variant[T >: TypeDecl.Normal](
      name: QualifiedName,
      tyVars: ImmArraySeq[Ast.TypeVarName],
      variant: Ast.DataVariant,
  ) = {
    for {
      cons <- fieldsOrCons(name, variant.variants)
    } yield name -> (TypeDecl.Normal(DefDataType(tyVars, Variant(cons))): T)
  }

  private[reader] def enumeration[T >: TypeDecl.Normal](
      name: QualifiedName,
      tyVars: ImmArraySeq[Ast.TypeVarName],
      enumeration: Ast.DataEnum,
  ): Error \/ (QualifiedName, T) =
    if (tyVars.isEmpty)
      \/-(
        name -> TypeDecl.Normal(
          DefDataType(ImmArraySeq.empty, Enum(enumeration.constructors.toSeq))
        )
      )
    else
      invalidDataTypeDefinition(name, s"non-empty type parameters for enum type $name")

  private[reader] def fieldsOrCons(
      ctx: QualifiedName,
      fields: ImmArray[(Ref.Name, Ast.Type)],
  ): Error \/ ImmArraySeq[(Ref.Name, Type)] =
    fields.toSeq traverse { case (fieldName, typ) =>
      toIfaceType(ctx, typ).map(x => fieldName -> x)
    }

  private[this] def interface(
      name: QualifiedName,
      astIf: Ast.DefInterface,
  ): Error \/ (QualifiedName, DefInterface.FWT) = for {
    choices <- astIf.choices.traverse(visitChoice(name, _))
    rawViewType <- toIfaceType(name, astIf.view)
    viewType <- rawViewType match {
      case TypeCon(TypeConName(tcn), Seq()) => \/-(Some(tcn))
      case TypePrim(PrimType.Unit, _) => \/-(None)
      case _ =>
        invalidDataTypeDefinition(
          name,
          s"interface view type ${astIf.view.pretty} must be either a no-argument type reference or unit",
        )
    }
    retroImplements = astIf.coImplements.keySet
  } yield name -> typesig.DefInterface(choices, retroImplements, viewType)

  private[lf] def toIfaceType(
      ctx: QualifiedName,
      a: Ast.Type,
      args: FrontStack[Type] = FrontStack.empty,
  ): Error \/ Type =
    a match {
      case Ast.TVar(x) =>
        if (args.isEmpty)
          \/-(TypeVar(x))
        else
          unserializableDataType(ctx, "arguments passed to a type parameter")
      case Ast.TTyCon(c) =>
        \/-(TypeCon(TypeConName(c), args.toImmArray.toSeq))
      case AstUtil.TNumeric(Ast.TNat(n)) if args.empty =>
        \/-(TypeNumeric(n))
      case Ast.TBuiltin(bt) =>
        primitiveType(ctx, bt, args.toImmArray.toSeq)
      case Ast.TApp(tyfun, arg) =>
        toIfaceType(ctx, arg, FrontStack.empty) flatMap (tArg =>
          toIfaceType(ctx, tyfun, tArg +: args)
        )
      case Ast.TForall(_, _) | Ast.TStruct(_) | Ast.TNat(_) | Ast.TSynApp(_, _) =>
        unserializableDataType(ctx, s"unserializable data type: ${a.pretty}")
    }

  private def primitiveType[T >: TypePrim](
      ctx: QualifiedName,
      a: Ast.BuiltinType,
      args: ImmArraySeq[Type],
  ): Error \/ T = {
    type Eo[A] = Error \/ A
    for {
      ab <- (a match {
        case Ast.BTUnit => \/-((0, PrimType.Unit))
        case Ast.BTBool => \/-((0, PrimType.Bool))
        case Ast.BTInt64 => \/-((0, PrimType.Int64))
        case Ast.BTText => \/-((0, PrimType.Text))
        case Ast.BTDate => \/-((0, PrimType.Date))
        case Ast.BTTimestamp => \/-((0, PrimType.Timestamp))
        case Ast.BTParty => \/-((0, PrimType.Party))
        case Ast.BTContractId => \/-((1, PrimType.ContractId))
        case Ast.BTList => \/-((1, PrimType.List))
        case Ast.BTOptional => \/-((1, PrimType.Optional))
        case Ast.BTTextMap => \/-((1, PrimType.TextMap))
        case Ast.BTGenMap => \/-((2, PrimType.GenMap))
        case Ast.BTNumeric =>
          unserializableDataType(
            ctx,
            s"Unserializable primitive type: $a must be applied to one and only one TNat",
          )
        case Ast.BTUpdate | Ast.BTScenario | Ast.BTArrow | Ast.BTAny | Ast.BTTypeRep |
            Ast.BTAnyException | Ast.BTBigNumeric | Ast.BTRoundingMode =>
          unserializableDataType(ctx, s"Unserializable primitive type: $a")
      }): Eo[(Int, PrimType)]
      (arity, primType) = ab
      typ <- {
        if (args.length != arity)
          invalidDataTypeDefinition(ctx, s"$a requires $arity arguments, but got ${args.length}")
        else
          \/-(TypePrim(primType, args))
      }: Eo[T]
    } yield typ
  }

}
