// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package iface
package reader

import ErrorFormatter._
import com.digitalasset.daml_lf.{DamlLf, DamlLf1}
import scalaz._
import scalaz.syntax.std.either._
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.std.list._
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  DottedName,
  Identifier,
  ModuleName,
  Name,
  PackageId,
  QualifiedName
}
import com.digitalasset.daml.lf.iface.TemplateChoice.FWT

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

object InterfaceReader {
  import Errors._, iface.{Interface, InterfaceType}

  sealed abstract class InterfaceReaderError extends Product with Serializable
  final case class UnserializableDataType(error: String) extends InterfaceReaderError
  final case class InvalidDataTypeDefinition(error: String) extends InterfaceReaderError

  object InterfaceReaderError {
    type Tree = Errors[ErrorLoc, InterfaceReaderError]

    implicit def `IRE semigroup`: Semigroup[InterfaceReaderError] =
      Semigroup.firstSemigroup

    def treeReport(errors: Errors[ErrorLoc, InterfaceReader.InvalidDataTypeDefinition]): Cord =
      stringReport(errors)(_ fold (sy => Cord(".") :+ sy.name, Cord("'") :+ _ :+ "'"), _.error)
  }

  private[reader] final case class Context(packageId: PackageId)

  private[reader] final case class State(
      typeDecls: Map[QualifiedName, InterfaceType] = Map.empty,
      errors: InterfaceReaderError.Tree = mzero[InterfaceReaderError.Tree]) {

    def addVariant(k: QualifiedName, tyVars: ImmArraySeq[Ref.Name], a: Variant.FWT): State =
      this.copy(typeDecls = this.typeDecls.updated(k, InterfaceType.Normal(DefDataType(tyVars, a))))

    def removeRecord(k: QualifiedName): Option[(Record.FWT, State)] =
      this.typeDecls.get(k).flatMap {
        case InterfaceType.Normal(DefDataType(_, rec: Record.FWT)) =>
          Some((rec, this.copy(typeDecls = this.typeDecls - k)))
        case _ => None
      }

    def addTemplate(k: QualifiedName, rec: Record.FWT, a: DefTemplate.FWT): State =
      this.copy(typeDecls = this.typeDecls.updated(k, InterfaceType.Template(rec, a)))

    def addError(e: InterfaceReaderError.Tree): State = alterErrors(_ |+| e)

    def alterErrors(e: InterfaceReaderError.Tree => InterfaceReaderError.Tree): State =
      copy(errors = e(errors))

    def asOut(packageId: PackageId): Interface = Interface(packageId, this.typeDecls)
  }

  private[reader] object State {
    implicit val stateMonoid: Monoid[State] =
      Monoid instance ((l, r) => State(l.typeDecls ++ r.typeDecls, l.errors |+| r.errors), State())
  }

  def readInterface(lf: DamlLf.Archive): (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) =
    readInterface(() => DamlLfV1ArchiveReader.readPackage(lf))

  def readInterface(lf: (PackageId, DamlLf.ArchivePayload))
    : (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) = {
    readInterface(() => lf.traverseU(DamlLfV1ArchiveReader.readPackage))
  }

  private val dummyPkgId = PackageId.assertFromString("-dummyPkg-")

  private val dummyInterface = Interface(dummyPkgId, Map.empty)

  def readInterface(f: () => String \/ (PackageId, DamlLf1.Package))
    : (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) =
    f() match {
      case -\/(e) =>
        (point(InvalidDataTypeDefinition(e)), dummyInterface)
      case \/-((templateGroupId, lfPackage)) =>
        lfprintln(s"templateGroupId: $templateGroupId")
        lfprintln(s"package: $lfPackage")
        val ctx: Context = Context(templateGroupId)
        val s: State = {
          import scalaz.std.iterable._
          lfPackage.getModulesList.asScala
            .foldMap(foldModule(_, ctx))
        }
        val r = (filterOutUnserializableErrors(s.errors), s.asOut(templateGroupId))
        lfprintln(s"result: $r")
        r
    }

  private def filterOutUnserializableErrors(
      es: InterfaceReaderError.Tree): Errors[ErrorLoc, InvalidDataTypeDefinition] =
    es.collectAndPrune { case x: InvalidDataTypeDefinition => x }

  private[reader] def foldModule(a: DamlLf1.Module, ctx: Context): State = {
    val partitions = Partitions(a)
    locate('name, rootErrOf[ErrorLoc](moduleName(a))) fold (
      e => State(errors = partitions.errorTree |+| e), { n =>
        val (recordErrs, typeDecls) = partitionIndexedErrs(partitions.records)(record(n, ctx))
        val z0 = State(typeDecls = typeDecls.map {
          case (_, (k, typVars, a)) => (k, InterfaceType.Normal(DefDataType(typVars, a)))
        }, errors = partitions.errorTree |+| recordErrs)
        val z1 = partitions.templates.foldLeft(z0)(foldTemplate(n, ctx))
        foldVariants(
          z1,
          partitionIndexedErrs(partitions.variants)(variant(n, ctx)) rightMap (_.values))
          .alterErrors(_ locate n)
      }
    )
  }

  private[reader] def moduleName(a: DamlLf1.Module): InterfaceReaderError \/ ModuleName =
    dottedName(a.getName)

  private[reader] def dottedName(a: DamlLf1.DottedName): InterfaceReaderError \/ DottedName =
    DottedName.fromSegments(a.getSegmentsList.asScala) match {
      case Left(err) => -\/(invalidDataTypeDefinition(a, s"Couldn't parse dotted name: $err"))
      case Right(x) => \/-(x)
    }

  private[reader] def packageId(a: DamlLf1.PackageRef): InterfaceReaderError \/ PackageId =
    PackageId.fromString(a.getPackageId).disjunction leftMap (err =>
      invalidDataTypeDefinition(a, s"Invalid packageId : $err"))

  private[this] def addPartitionToState[A](
      state: State,
      a: (InterfaceReaderError.Tree, Iterable[A]))(f: (State, A) => State): State =
    a._2.foldLeft(state addError a._1)(f)

  private[reader] def record(m: ModuleName, ctx: Context)(a: DamlLf1.DefDataType)
    : InterfaceReaderError.Tree \/ (QualifiedName, ImmArraySeq[Ref.Name], Record.FWT) =
    recordOrVariant(m, a, _.getRecord, ctx) { (k, tyVars, fields) =>
      (k, tyVars.toSeq, Record(fields.toSeq))
    }

  private def foldVariants(
      state: State,
      a: (InterfaceReaderError.Tree, Iterable[(QualifiedName, ImmArraySeq[Ref.Name], Variant.FWT)]))
    : State =
    addPartitionToState(state, a) {
      case (st, (k, typVars, variant)) => st.addVariant(k, typVars, variant)
    }

  private[reader] def variant(m: ModuleName, ctx: Context)(a: DamlLf1.DefDataType)
    : InterfaceReaderError.Tree \/ (QualifiedName, ImmArraySeq[Ref.Name], Variant.FWT) =
    recordOrVariant(m, a, _.getVariant, ctx) { (k, tyVars, fields) =>
      (k, tyVars.toSeq, Variant(fields.toSeq))
    }

  private[this] def recordOrVariant[Z](
      m: ModuleName,
      a: DamlLf1.DefDataType,
      getSum: DamlLf1.DefDataType => DamlLf1.DefDataType.Fields,
      ctx: Context)(mk: (QualifiedName, ImmArray[Ref.Name], ImmArray[FieldWithType]) => Z)
    : InterfaceReaderError.Tree \/ Z =
    (locate('name, rootErrOf[ErrorLoc](fullName(m, a.getName))).validation |@|
      locate('typeParams, typeParams(a)).validation |@|
      locate('fields, fields(getSum(a), ctx)).validation)(mk).disjunction

  private[reader] def foldTemplate(m: ModuleName, ctx: Context)(
      state: State,
      a: DamlLf1.DefTemplate): State =
    locate('tycon, rootErrOf[ErrorLoc](fullName(m, a.getTycon))).fold(
      state.addError, { templateName =>
        state.removeRecord(templateName) match {
          case None =>
            state.addError(
              point(InvalidDataTypeDefinition(
                s"Cannot find a record associated with template: $templateName")))
          case Some((rec, newState)) =>
            val y: Errors[ErrorLoc, InterfaceReaderError] \/ Map[ChoiceName, FWT] =
              locate('choices, choices(a, ctx))

            y.fold(
              newState.addError, { cs =>
                newState.addTemplate(templateName, rec, DefTemplate(cs))
              }
            )
        }
      }
    )

  private def name(s: String): InvalidDataTypeDefinition \/ Name =
    Name.fromString(s).disjunction leftMap InvalidDataTypeDefinition

  private def choices(
      a: DamlLf1.DefTemplate,
      ctx: Context
  ): InterfaceReaderError.Tree \/ Map[ChoiceName, TemplateChoice.FWT] = {

    val z: Errors[ErrorLoc, InterfaceReaderError] \/ List[(Name, DamlLf1.TemplateChoice)] =
      locate(
        'choices,
        rootErr(a.getChoicesList.asScala.toList.traverseU(a => name(a.getName).map(_ -> a))))

    z flatMap (z => traverseIndexedErrsMap(z.toMap)(c => rootErr(visitChoice(c, ctx))))
  }

  private def visitChoice(
      a: DamlLf1.TemplateChoice,
      ctx: Context): InterfaceReaderError \/ TemplateChoice.FWT =
    for {
      p <- type_(a.getArgBinder.getType, ctx)
      r <- type_(a.getRetType, ctx)
      choice = TemplateChoice(p, consuming = a.getConsuming, returnType = r)
    } yield choice

  private def fullName(
      m: ModuleName,
      a: DamlLf1.DottedName): InterfaceReaderError \/ QualifiedName =
    dottedName(a) map (a => QualifiedName(m, a))

  private def showKind(a: DamlLf1.Kind): String =
    a.toString // or something nicer

  private def typeVarRef(a: DamlLf1.Type.Var): InterfaceReaderError \/ Ref.Name =
    if (a.getArgsList.isEmpty) name(a.getVar)
    else -\/(unserializableDataType(a, "arguments passed to a type parameter"))

  private def typeVar(a: DamlLf1.TypeVarWithKind): InterfaceReaderError \/ Ref.Name = {
    import DamlLf1.Kind.{SumCase => TSC}
    a.getKind.getSumCase match {
      case TSC.STAR => name(a.getVar)
      case TSC.ARROW =>
        -\/(UnserializableDataType(s"non-star-kinded type variable: ${showKind(a.getKind)}"))
      case TSC.SUM_NOT_SET =>
        -\/(InvalidDataTypeDefinition("DamlLf1.Kind.SumCase.SUM_NOT_SET"))
    }
  }

  private def typeParams(a: DamlLf1.DefDataType): InterfaceReaderError.Tree \/ ImmArray[Ref.Name] =
    traverseIndexedErrs(ImmArray(a.getParamsList.asScala).map(tvwk => (tvwk.getVar, tvwk)))(tvwk =>
      rootErr(typeVar(tvwk)))

  private def fields(
      as: DamlLf1.DefDataType.Fields,
      ctx: Context): InterfaceReaderError.Tree \/ ImmArray[FieldWithType] =
    traverseIndexedErrs(ImmArray(as.getFieldsList.asScala).map(a => (a.getField, a)))(f =>
      rootErr(fieldWithType(f, ctx)))

  private def fieldWithType(
      a: DamlLf1.FieldWithType,
      ctx: Context): InterfaceReaderError \/ FieldWithType =
    type_(a.getType, ctx).flatMap(t => name(a.getField).map(_ -> t))

  /**
    * `Fun`, `Forall` and `Tuple` should never appear in Records and Variants
    */
  private def type_(a: DamlLf1.Type, ctx: Context): InterfaceReaderError \/ Type = {
    import DamlLf1.Type.{SumCase => TSC}
    a.getSumCase match {
      case TSC.VAR => typeVarRef(a.getVar) map (TypeVar(_))
      case TSC.CON => typeConApp(a.getCon, ctx)
      case TSC.PRIM => primitiveType(a.getPrim, ctx)
      case sc @ (TSC.FUN | TSC.FORALL | TSC.TUPLE) =>
        -\/(unserializableDataType(a, s"Unserializable data type: DamlLf1.Type.SumCase.${sc.name}"))
      case TSC.SUM_NOT_SET =>
        -\/(invalidDataTypeDefinition(a, "DamlLf1.Type.SumCase.SUM_NOT_SET"))
    }
  }

  private def typeConApp(a: DamlLf1.Type.Con, ctx: Context): InterfaceReaderError \/ Type = {
    for {
      typeName <- typeConName(a.getTycon, ctx)
      typeArgs <- ImmArray(a.getArgsList.asScala)
        .traverseU(t => type_(t, ctx)): InterfaceReaderError \/ ImmArray[Type]
    } yield TypeCon(typeName, typeArgs.toIndexedSeq)
  }

  private def typeConName(
      a: DamlLf1.TypeConName,
      ctx: Context): InterfaceReaderError \/ TypeConName =
    (moduleRef(a.getModule) |@| dottedName(a.getName)) {
      case ((pkgId, mname), name) =>
        TypeConName(Identifier(pkgId.getOrElse(ctx.packageId), QualifiedName(mname, name)))
    }

  private def moduleRef(
      a: DamlLf1.ModuleRef): InterfaceReaderError \/ (Option[PackageId], ModuleName) =
    (packageRef(a.getPackageRef) |@| dottedName(a.getModuleName)) { (pkgId, mname) =>
      (pkgId, mname)
    }

  private def packageRef(a: DamlLf1.PackageRef): InterfaceReaderError \/ Option[PackageId] =
    a.getSumCase match {
      case DamlLf1.PackageRef.SumCase.SELF => \/-(None)
      case DamlLf1.PackageRef.SumCase.PACKAGE_ID =>
        packageId(a).map(Some(_))
      case DamlLf1.PackageRef.SumCase.SUM_NOT_SET =>
        -\/(invalidDataTypeDefinition(a, "DamlLf1.PackageRef.SumCase.SUM_NOT_SET"))
    }

  private def primitiveType(a: DamlLf1.Type.Prim, ctx: Context): InterfaceReaderError \/ Type = {
    import DamlLf1.{PrimType => PT}
    val pt = a.getPrim
    val t: InterfaceReaderError \/ (Int, PrimType) = pt match {
      case PT.UNIT => \/-((0, PrimType.Unit))
      case PT.BOOL => \/-((0, PrimType.Bool))
      case PT.INT64 => \/-((0, PrimType.Int64))
      case PT.DECIMAL => \/-((0, PrimType.Decimal))
      case PT.TEXT => \/-((0, PrimType.Text))
      case PT.DATE => \/-((0, PrimType.Date))
      case PT.TIMESTAMP => \/-((0, PrimType.Timestamp))
      case PT.PARTY => \/-((0, PrimType.Party))
      case PT.LIST => \/-((1, PrimType.List))
      case PT.CONTRACT_ID => \/-((1, PrimType.ContractId))
      case PT.OPTIONAL => \/-((1, PrimType.Optional))
      case PT.MAP => \/-((1, PrimType.Map))
      case PT.UPDATE | PT.SCENARIO | PT.ARROW =>
        -\/(unserializableDataType(a, s"Unserializable data type: DamlLf1.PrimType.${pt.name}"))
      case PT.UNRECOGNIZED =>
        -\/(invalidDataTypeDefinition(a, "DamlLf1.PrimType.UNRECOGNIZED"))
    }
    t.flatMap {
      case (arity, bt) =>
        val args = ImmArray(a.getArgsList.asScala)
        if (args.length != arity)
          -\/(
            invalidDataTypeDefinition(
              a,
              s"${pt.name} requires $arity arguments, but got ${args.length}"))
        else args traverseU (t => type_(t, ctx)) map (xs => TypePrim(bt, xs.toIndexedSeq))
    }
  }

}
