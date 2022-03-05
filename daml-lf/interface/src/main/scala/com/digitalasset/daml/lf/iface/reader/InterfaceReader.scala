// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface
package reader

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.ArchivePayload
import scalaz.{Enum => _, _}
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.option._
import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.language.Ast
import com.daml.lf.language.{Util => AstUtil}

import scala.collection.immutable.Map

object InterfaceReader {
  import Errors._

  sealed abstract class InterfaceReaderError extends Product with Serializable
  final case class UnserializableDataType(error: String) extends InterfaceReaderError
  final case class InvalidDataTypeDefinition(error: String) extends InterfaceReaderError

  private def errorMessage(ctx: QualifiedName, reason: String): String =
    s"Invalid data definition: $ctx, reason: $reason"

  private def invalidDataTypeDefinition[Bot](
      ctx: QualifiedName,
      reason: String,
  ): InterfaceReaderError \/ Bot = -\/(InvalidDataTypeDefinition(errorMessage(ctx, reason)))

  private def unserializableDataType[Bot](
      ctx: QualifiedName,
      reason: String,
  ): InterfaceReaderError \/ Bot = -\/(UnserializableDataType(errorMessage(ctx, reason)))

  object InterfaceReaderError {
    type Tree = Errors[ErrorLoc, InterfaceReaderError]

    implicit def `IRE semigroup`: Semigroup[InterfaceReaderError] =
      Semigroup.firstSemigroup

    def treeReport(errors: Errors[ErrorLoc, InterfaceReader.InvalidDataTypeDefinition]): Cord =
      stringReport(errors)(
        _.fold(prop => Cord(s".${prop.name}"), ixName => Cord(s"'$ixName'")),
        e => Cord(e.error),
      )
  }

  private[reader] final case class State(
      typeDecls: Map[QualifiedName, iface.InterfaceType] = Map.empty,
      astInterfaces: Map[QualifiedName, iface.DefInterface.FWT] = Map.empty,
      errors: InterfaceReaderError.Tree = mzero[InterfaceReaderError.Tree],
  ) {

    def addTypeDecl(nd: (Ref.QualifiedName, iface.InterfaceType)): State =
      copy(typeDecls + nd)

    def addError(e: InterfaceReaderError.Tree): State = alterErrors(_ |+| e)

    def alterErrors(e: InterfaceReaderError.Tree => InterfaceReaderError.Tree): State =
      copy(errors = e(errors))

    def asOut(packageId: PackageId, metadata: Option[PackageMetadata]): iface.Interface =
      iface.Interface(packageId, metadata, typeDecls, astInterfaces)
  }

  private[reader] object State {
    implicit val stateMonoid: Monoid[State] =
      Monoid.instance(
        (l, r) =>
          State(
            l.typeDecls ++ r.typeDecls,
            l.astInterfaces ++ r.astInterfaces,
            l.errors |+| r.errors,
          ),
        State(),
      )
  }

  def readInterface(
      lf: DamlLf.Archive
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], iface.Interface) =
    readInterface(() => DamlLfArchiveReader.readPackage(lf))

  def readInterface(
      packageId: Ref.PackageId,
      damlLf: DamlLf.ArchivePayload,
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], iface.Interface) =
    readInterface(() => DamlLfArchiveReader.readPackage(packageId, damlLf))

  def readInterface(
      payload: ArchivePayload
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], iface.Interface) =
    readInterface(() => DamlLfArchiveReader.readPackage(payload))

  private val dummyPkgId = PackageId.assertFromString("-dummyPkg-")

  private val dummyInterface = iface.Interface(dummyPkgId, None, Map.empty, Map.empty)

  def readInterface(
      f: () => String \/ (PackageId, Ast.Package)
  ): (Errors[ErrorLoc, InvalidDataTypeDefinition], iface.Interface) =
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
      es: InterfaceReaderError.Tree
  ): Errors[ErrorLoc, InvalidDataTypeDefinition] =
    es.collectAndPrune { case x: InvalidDataTypeDefinition => x }

  private[reader] def foldModule(module: Ast.Module): State = {
    val (errors, elements) = (module.definitions: Iterable[(Ref.DottedName, Ast.Definition)])
      .collect { case (name, Ast.DDataType(true, params, dataType)) =>
        val fullName = QualifiedName(module.name, name)
        val tyVars: ImmArraySeq[Ast.TypeVarName] = params.map(_._1).toSeq

        val result: InterfaceReaderError \/ Either[
          (QualifiedName, DefInterface.FWT),
          (QualifiedName, iface.InterfaceType),
        ] =
          dataType match {
            case dfn: Ast.DataRecord =>
              val it = module.templates.get(name) match {
                case Some(tmpl) => template(fullName, dfn, tmpl)
                case None => record(fullName, tyVars, dfn)
              }
              it.map(Right(_))
            case dfn: Ast.DataVariant =>
              variant(fullName, tyVars, dfn).map(Right(_))
            case dfn: Ast.DataEnum =>
              enumeration(fullName, tyVars, dfn).map(Right(_))
            case Ast.DataInterface =>
              module.interfaces
                .get(name)
                .toRightDisjunction(
                  InvalidDataTypeDefinition(
                    s"missing interface definition: $fullName"
                  ): InterfaceReaderError
                )
                .flatMap(astInterface(fullName, _))
                .map(Left(_))
          }
        locate(Symbol("name"), rootErrOf[ErrorLoc](result)).toEither
      }
      .partitionMap(identity)
    val (astIfs, ddts) = elements partitionMap identity

    import scalaz.std.iterable._
    State(typeDecls = ddts.toMap, astInterfaces = astIfs.toMap, errors = errors.suml)
  }

  private[reader] def record[T >: iface.InterfaceType.Normal](
      name: QualifiedName,
      tyVars: ImmArraySeq[Ast.TypeVarName],
      record: Ast.DataRecord,
  ) =
    for {
      fields <- fieldsOrCons(name, record.fields)
    } yield name -> (iface.InterfaceType.Normal(DefDataType(tyVars, Record(fields))): T)

  private[reader] def template[T >: iface.InterfaceType.Template](
      name: QualifiedName,
      record: Ast.DataRecord,
      dfn: Ast.Template,
  ) =
    for {
      fields <- fieldsOrCons(name, record.fields)
      choices <- dfn.choices.toList traverse { case (choiceName, choice) =>
        visitChoice(name, choice) map (x => choiceName -> x)
      }
      key <- dfn.key traverse (k => toIfaceType(name, k.typ))
    } yield name -> (iface.InterfaceType.Template(
      Record(fields),
      DefTemplate(choices.toMap, key),
    ): T)

  private def visitChoice(
      ctx: QualifiedName,
      choice: Ast.TemplateChoice,
  ): InterfaceReaderError \/ TemplateChoice[Type] =
    for {
      tParam <- toIfaceType(ctx, choice.argBinder._2)
      tReturn <- toIfaceType(ctx, choice.returnType)
    } yield TemplateChoice(
      param = tParam,
      consuming = choice.consuming,
      returnType = tReturn,
    )

  private[reader] def variant[T >: iface.InterfaceType.Normal](
      name: QualifiedName,
      tyVars: ImmArraySeq[Ast.TypeVarName],
      variant: Ast.DataVariant,
  ) = {
    for {
      cons <- fieldsOrCons(name, variant.variants)
    } yield name -> (iface.InterfaceType.Normal(DefDataType(tyVars, Variant(cons))): T)
  }

  private[reader] def enumeration[T >: iface.InterfaceType.Normal](
      name: QualifiedName,
      tyVars: ImmArraySeq[Ast.TypeVarName],
      enumeration: Ast.DataEnum,
  ): InterfaceReaderError \/ (QualifiedName, T) =
    if (tyVars.isEmpty)
      \/-(
        name -> iface.InterfaceType.Normal(
          DefDataType(ImmArraySeq.empty, Enum(enumeration.constructors.toSeq))
        )
      )
    else
      invalidDataTypeDefinition(name, s"non-empty type parameters for enum type $name")

  private[reader] def fieldsOrCons(
      ctx: QualifiedName,
      fields: ImmArray[(Ref.Name, Ast.Type)],
  ): InterfaceReaderError \/ ImmArraySeq[(Ref.Name, Type)] =
    fields.toSeq traverse { case (fieldName, typ) =>
      toIfaceType(ctx, typ).map(x => fieldName -> x)
    }

  private[this] def astInterface(
      name: QualifiedName,
      astIf: Ast.DefInterface,
  ) = for {
    choices <- astIf.fixedChoices.traverse(visitChoice(name, _))
  } yield name -> iface.DefInterface(choices)

  private[lf] def toIfaceType(
      ctx: QualifiedName,
      a: Ast.Type,
      args: FrontStack[Type] = FrontStack.empty,
  ): InterfaceReaderError \/ Type =
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
  ): InterfaceReaderError \/ T = {
    type Eo[A] = InterfaceReaderError \/ A
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
