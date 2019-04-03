// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.lf

import com.digitalasset.{codegen => parent}
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier, QualifiedName}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import parent.dependencygraph.DependencyGraph
import parent.exception.UnsupportedDamlTypeException
import com.digitalasset.daml.lf.iface
import iface.{
  PrimType => PT,
  PrimTypeOptional => PTOptional,
  PrimTypeMap => PTMap,
  Type => IType,
  _
}
import com.digitalasset.daml.lf.iface.reader.InterfaceType

import java.io.File
import scalaz._
import scalaz.std.set._
import scalaz.syntax.id._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._

case class DefTemplateWithRecord[+Type](`type`: Record[(String, Type)], template: DefTemplate[Type])
object DefTemplateWithRecord {
  type FWT = DefTemplateWithRecord[IType]
}

/**
  *  In order to avoid endlessly passing around "packageName" and "iface" to
  *  utility functions we initialise a class with these values and allow all the
  *  methods to have access to them.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
final case class LFUtil(
    override val packageName: String,
    override val iface: EnvironmentInterface,
    override val outputDir: File)
    extends parent.Util(packageName, outputDir) {

  import scala.reflect.runtime.universe._
  import parent.Util._
  import LFUtil._

  type Interface = EnvironmentInterface

  type TemplateInterface = DefTemplateWithRecord.FWT

  override val mode = parent.CodeGen.Novel

  private[codegen] override def orderedDependencies(library: Interface) =
    DependencyGraph(this).orderedDependencies(library)

  override def templateAndTypeFiles(wp: WriteParams[TemplateInterface]) =
    parent.CodeGen.produceTemplateAndTypeFilesLF(wp, this)

  // XXX DamlScalaName doesn't depend on packageId at the moment, but
  // there are good reasons to make it do so
  def mkDamlScalaName(codeGenDeclKind: CodeGenDeclKind, metadataAlias: Identifier): DamlScalaName =
    mkDamlScalaName(codeGenDeclKind, metadataAlias.qualifiedName)

  def mkDamlScalaName(
      codeGenDeclKind: CodeGenDeclKind,
      metadataAlias: QualifiedName): DamlScalaName = {
    val (damlNameSpace, name) = qualifiedNameToDirsAndName(metadataAlias)
    mkDamlScalaNameFromDirsAndName(damlNameSpace, name.capitalize)
  }

  def mkDamlScalaNameFromDirsAndName(nameSpace: Array[String], name: String): DamlScalaName = {
    DamlScalaName(nameSpace, name)
  }

  /**
    *  This method is responsible for generating Scala reflection API "tree"s.
    *  It maps from Core package interface types to:
    *  a) the wrapper types defined in the typed-ledger-api project
    *  b) the classes that will be generated for the user defined types defined in
    *     the "typeDecls" field of the Package.PackageInterface value.
    */
  def genTypeToScalaType(genType: IType): Tree = {
    def refTypeToIdent(refType: TypeConNameOrPrimType) =
      refType match {
        case PT.Bool => q"$primitiveObject.Bool"
        case PT.Int64 => q"$primitiveObject.Int64"
        case PT.Decimal => q"$primitiveObject.Decimal"
        case PT.Party => q"$primitiveObject.Party"
        case PT.Text => q"$primitiveObject.Text"
        case PT.Date => q"$primitiveObject.Date"
        case PT.Timestamp => q"$primitiveObject.Timestamp"
        case PT.Unit => q"$primitiveObject.Unit"
        // TODO add PT.Optional alias to iface, use here
        case PT.List | PT.ContractId | PTOptional | PTMap =>
          throw UnsupportedDamlTypeException(
            s"type $refType should not occur in a Data-kinded position; this is an invalid input LF")
        case TypeConName(tyCon) =>
          val damlScalaName = mkDamlScalaName(UserDefinedType, tyCon)
          damlScalaName.toRefTree
      }

    genType match {
      case TypePrim(PT.List, ImmArraySeq(typ)) =>
        val listType = genTypeToScalaType(typ)
        q"$primitiveObject.List[$listType]"
      case TypePrim(PT.ContractId, ImmArraySeq(typ)) =>
        val templateType = genTypeToScalaType(typ)
        q"$primitiveObject.ContractId[$templateType]"
      case TypePrim(PTOptional, ImmArraySeq(typ)) =>
        val optType = genTypeToScalaType(typ)
        q"$primitiveObject.Optional[$optType]"
      case TypePrim(PTMap, ImmArraySeq(typ)) =>
        val optType = genTypeToScalaType(typ)
        q"$primitiveObject.Map[$optType]"
      case TypePrim(refType, ImmArraySeq()) => refTypeToIdent(refType)
      case TypeCon(name, ImmArraySeq()) => refTypeToIdent(name)
      case TypePrim(refType, typeArgs) =>
        TypeApply(refTypeToIdent(refType), typeArgs.toList map genTypeToScalaType)
      case TypeCon(name, typeArgs) =>
        TypeApply(refTypeToIdent(name), typeArgs.toList map genTypeToScalaType)
      case TypeVar(name) => toIdent(name)
    }
  }

  def genArgsWithTypes(fields: Seq[FieldWithType]): Seq[Typed] = fields.map {
    case (label, typ) =>
      q"${toIdent(escapeIfReservedName(label))}: ${genTypeToScalaType(typ)}"
  }

  /** Produces the for-comprehension body of a "reader" method.
    *
    * Example: Take two variables "a: Int64", "b: Decimal", a nameOfRecord of "m"
    * This will produce:
    *
    *   if (m.fields == 2) {
    *     for {
    *       RecordField("" | "a", Some(z0)) = m.fields(0)
    *       a <- Value.decode[Int64](zv0)
    *       RecordField("" | "b", Some(zv1)) = m.fields(1)
    *       b <- Value.decode[Decimal](zv1)
    *     } yield (...) // what the user passes in
    *   } else {
    *     None
    *   }
    *
    * Note that we index and pattern match rather than pattern matching on Seq because
    * unapply patterns are limited to 22 arguments, and we might have records with more
    * than 22 arguments.
    */
  @throws[UnsupportedDamlTypeException]
  def genForComprehensionBodyOfReaderMethod(
      params: Seq[FieldWithType],
      paramIds: Iterable[Ident],
      nameOfRecord: String,
      inner: Tree): Tree = {
    require(params.size == paramIds.size)
    val numFields = params.size
    val zvs = generateIds(params.size, "zv")
    // we match explicitly to avoid calling flatMap at each step with the
    // for comprehension
    val matching = (params zip (zvs zip paramIds)).zipWithIndex.foldRight(q"Some($inner)") {
      case ((((name, genType), (zv, z)), ix), acc) =>
        q"""
          ${TermName(nameOfRecord)}.fields($ix) match {
            case $rpcValueAlias.RecordField("" | $name, _root_.scala.Some($zv)) =>
              ${genArgumentValueToGenType(genType)}($zv) match {
                case _root_.scala.Some($z) => $acc
                case _root_.scala.None => _root_.scala.None
              }
            case _ => _root_.scala.None
          }
        """
    }
    q"""
      if (${TermName(nameOfRecord)}.fields.length == $numFields) {
        $matching
      } else {
        _root_.scala.None
      }
    """
  }

  override def genArgumentValueToGenType(genType: IType): Tree = {
    val typTree = genTypeToScalaType(genType)
    q"$domainApiAlias.Value.decode[$typTree]"
  }

  override def paramRefAndGenTypeToArgumentValue(paramRef: Tree, genType: IType): Tree =
    q"$domainApiAlias.Value.encode($paramRef)"

  def toNamedArgumentsMap(params: List[FieldWithType], path: Option[Tree]): Tree = {
    val ps = params.map {
      case (n, t) =>
        val unqualified = TermName(n)
        val pr = path.cata(p => q"$p.$unqualified", q"$unqualified")
        q"($n, ${paramRefAndGenTypeToArgumentValue(pr, t)})"
    }
    q"` arguments`(..$ps)"
  }

  def genTemplateChoiceMethods(
      choiceId: ChoiceName,
      choiceInterface: TemplateChoice.FWT): Seq[Tree] = {
    val choiceMethod = TermName(s"exercise$choiceId")
    val choiceParam = choiceInterface.param
    val choiceParamName = toIdent("choiceArgument")
    def nonunitaryCase(
        ty: IType,
        apn: String,
        dn: Option[(QualifiedName, ImmArraySeq[FieldWithType])]) =
      (
        Some(q"$choiceParamName: ${genTypeToScalaType(ty)}"),
        q"_root_.scala.Some(${paramRefAndGenTypeToArgumentValue(choiceParamName, ty)})",
        apn,
        dn)
    val (typedParam, namedArguments, actorParamName, denominalized1) =
      choiceParam match {
        case TypePrim(PT.Unit, ImmArraySeq()) => (None, q"_root_.scala.None", "actor", None)
        case TypeCon(TypeConName(tyCon), _) =>
          val dn =
            iface.typeDecls get tyCon collect {
              case InterfaceType.Normal(DefDataType(ImmArraySeq(), Record(fields))) => fields
            }
          dn map { fields =>
            val orecArgNames = fields.foldMap { case (fn, _) => Set(fn) }
            // pick a unique name for Party param among the choice params, if
            // and only if nominalized, by _-suffixing
            nonunitaryCase(
              choiceParam,
              "actor".whileDo(na => s"${na}_", orecArgNames),
              Some((tyCon.qualifiedName, fields)))
          } getOrElse {
            nonunitaryCase(choiceParam, "actor", None)
          }
        case ty =>
          nonunitaryCase(ty, "actor", None)
      }
    val denominalized = denominalized1.map {
      case (name, fields) =>
        (
          genArgsWithTypes(fields),
          mkDamlScalaName(UserDefinedType, name).qualifiedTermName,
          fields.map { case (label, _) => toIdent(escapeIfReservedName(label)) })
    }
    val actorParam = q"${TermName(actorParamName)}: $domainApiAlias.Primitive.Party"
    val resultType = genTypeToScalaType(choiceInterface.returnType)
    val body = q"` exercise`(id, $choiceId, $namedArguments)"

    Seq(q"""def $choiceMethod($actorParam, ..${typedParam.toList})
                : $domainApiAlias.Primitive.Update[$resultType] = $body""") ++
      denominalized.map {
        case (dparams, dctorName, dargs) =>
          q"""def $choiceMethod($actorParam, ..$dparams)
                  : $domainApiAlias.Primitive.Update[$resultType] =
                $choiceMethod(${TermName(actorParamName)}, $dctorName(..$dargs))"""
      }.toList
  }
}

object LFUtil {
  import scala.reflect.runtime.universe._

  private[this] val reservedNamePrefixes = {
    import scala.util.matching.Regex
    Seq(
      "_root_",
      "asInstanceOf",
      "getClass",
      "hashCode",
      "isInstanceOf",
      "notify",
      "notifyAll",
      "productArity",
      "productIterator",
      "productPrefix",
      "toString",
      "wait").map(Regex.quote).mkString("(?:", "|", ")_*").r.anchored
  }

  /** Either mangle a reserved name, or return it unchanged. Reserved
    * names are defined by the Scala interface section of the LF
    * specification.
    *
    * Note: '''not idempotent'''!  Escaped reserved names are ''still''
    * reserved, and will be escaped again by a second call.  See said
    * spec for details.
    */
  def escapeReservedName(name: String): String \/ name.type =
    name match {
      case reservedNamePrefixes(_*) => -\/(s"${name}_")
      case _ => \/-(name)
    }

  def escapeIfReservedName(name: String): String =
    escapeReservedName(name).fold(identity, identity)

  private[lf] def generateIds(number: Int, prefix: String): List[Ident] =
    List.fill(number)(prefix).zipWithIndex.map(t => parent.Util.toIdent(s"${t._1}${t._2}"))

  private[lf] def genConsumingChoicesMethod(templateInterface: DefTemplate[_]) = {
    val consumingChoicesIds = templateInterface.choices
      .collect {
        case (id, ci) if ci.consuming => id
      }
    q"override val consumingChoices: Set[$domainApiAlias.Primitive.ChoiceId] = $domainApiAlias.Primitive.ChoiceId.subst(Set(..$consumingChoicesIds))"
  }

  final case class TupleNesting[A](run: NonEmptyList[A \/ TupleNesting[A]])
      extends Product
      with Serializable {
    def map[B](f: A => B): TupleNesting[B] =
      TupleNesting(run map (_ bimap (f, _ map f)))

    // not tail recursive
    def fold[Z](pure: A => Z)(roll: NonEmptyList[Z] => Z): Z =
      roll(run map (_ fold (pure, _.fold(pure)(roll))))
  }

  /** Group `flat` into the shallowest permissible tree, given that a maximum of
    * `root` branches are permitted directly at the root and `subtrees` branches
    * are permitted from each internal node other than root.
    *
    * This is used for generating applicative-style code for which multiple
    * arities of `liftA`''n'' are supported and it is more
    * efficient/specializable to use higher arities when possible.  For example,
    * [[scalaz.Apply]] supports up to `apply12` and `tuple5`, so root=12,
    * subtrees=5 would be appropriate to generate calls to its functions.
    *
    * See `LFUtilSpec#tupleNestingSamples` for some example inputs and
    * output.
    */
  def tupleNesting[A](flat: NonEmptyList[A], root: Int, subtrees: Int): TupleNesting[A] = {
    require(root >= 2, s"root ($root) must be >= 2")
    require(subtrees >= 2, s"subtrees ($subtrees) must be >= 2")
    val totalArity = flat.size
    val rightCount = root min ((totalArity - root + subtrees - 2) / (subtrees - 1)) max 0
    val (lefts, ungroupedRights) = flat.list splitAt (root - rightCount)
    val ungroupedCount = totalArity - root + rightCount
    val groupedRights = if (rightCount > 0) unfoldIList((0, ungroupedRights)) {
      case (idx, remaining) =>
        val (here, remaining2) = remaining splitAt ((ungroupedCount + idx) / rightCount)
        here.toNel map ((_, (1 + idx, remaining2))) orElse {
          assert(remaining.isEmpty, "unfold must group all")
          None
        }
    } else IList.empty
    (lefts.map(\/.left[A, TupleNesting[A]])
      ++ groupedRights.map(g => \/.right(tupleNesting(g, subtrees, subtrees)))).toNel
      .map(TupleNesting(_))
      .getOrElse(throw new IllegalStateException(
        "impossible; either lefts or groupedRights must be non-empty"))
  }

  private[this] def unfoldIList[S, A](init: S)(step: S => Option[(A, S)]): IList[A] =
    step(init).fold(IList.empty[A]) { case (a, next) => a :: unfoldIList(next)(step) }

  /** Debugging message for LF codegen */
  @annotation.elidable(annotation.elidable.FINE)
  private[codegen] def lfprintln(s: String): Unit =
    println(s)

  val domainApiAlias = q"` lfdomainapi`"
  val rpcValueAlias = q"` rpcvalue`"
  val rpcEventAlias = q"` rpcevent`"
  val higherKindsImport: Tree =
    q"import _root_.scala.language.higherKinds"
  val domainApiImport: Tree =
    q"import _root_.com.digitalasset.ledger.client.{binding => ` lfdomainapi`}"
  val rpcValueImport: Tree =
    q"import _root_.com.digitalasset.ledger.api.v1.{value => ` rpcvalue`}"
  val rpcEventImport: Tree =
    q"import _root_.com.digitalasset.ledger.api.v1.{event => ` rpcevent`}"
  val primitiveObject: Tree = q"$domainApiAlias.Primitive"
  val stdMapType = tq"_root_.scala.collection.immutable.Map"
  val stdMapCompanion = q"_root_.scala.collection.immutable.Map"
  val stdSeqCompanion = q"_root_.scala.collection.immutable.Seq"

  def toTypeDef(s: String): TypeDef = q"type ${TypeName(s)}"

  def toCovariantTypeDef(s: String): TypeDef = {
    val TypeDef(Modifiers(flags, mname, mtrees), name, tparams, rhs) = toTypeDef(s)
    TypeDef(Modifiers(flags | Flag.COVARIANT, mname, mtrees), name, tparams, rhs)
  }
}
