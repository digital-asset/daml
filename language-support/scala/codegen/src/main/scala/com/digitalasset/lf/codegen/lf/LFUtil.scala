// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import com.daml.lf.{codegen => parent}
import com.daml.lf.data.Ref
import com.daml.lf.data.ImmArray.ImmArraySeq
import parent.exception.UnsupportedDamlTypeException
import com.daml.lf.typesig
import typesig.{Type => IType, PrimType => PT, _}
import PackageSignature.TypeDecl
import java.io.File

import com.daml.lf.codegen.dependencygraph.TransitiveClosure
import scalaz._
import scalaz.std.set._
import scalaz.syntax.id._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._

/**  In order to avoid endlessly passing around "packageName" and "iface" to
  *  utility functions we initialise a class with these values and allow all the
  *  methods to have access to them.
  */
final case class LFUtil(
    packageName: String,
    iface: EnvironmentSignature,
    outputDir: File,
) {

  import scala.reflect.runtime.universe._
  import LFUtil._

  def templateAndTypeFiles(wp: WriteParams) =
    parent.CodeGen.produceTemplateAndTypeFilesLF(wp, this)

  def mkDamlScalaName(
      metadataAlias: Ref.QualifiedName
  ): DamlScalaName = {
    val (damlNameSpace, name) = qualifiedNameToDirsAndName(metadataAlias)
    mkDamlScalaNameFromDirsAndName(damlNameSpace, name.capitalize)
  }

  def mkDamlScalaNameFromDirsAndName(nameSpace: Array[String], name: String): DamlScalaName = {
    DamlScalaName(nameSpace, name)
  }

  private[this] val packageNameElems: Array[String] = packageName.split('.')

  /** A Scala class/object package suffix and name.
    *
    * @param packageSuffixParts the package suffix of the class. This will be appended to
    *                           the [[packageNameElems]] to create the package for this
    *                           Scala class/object
    * @param name the name of the class/object
    */
  case class DamlScalaName(packageSuffixParts: Array[String], name: String) {

    /** Components of the package for this class/object */
    def packageNameParts = packageNameElems ++ packageSuffixParts

    /** The package for this class/object */
    def packageName = packageNameParts.mkString(".")

    /** Components of the fully qualified name for this class/object */
    def qualifiedNameParts = packageNameParts :+ name

    /** The fully qualified name of this class/object */
    def qualifiedName = qualifiedNameParts.mkString(".")

    private[this] def qualifierTree =
      packageNameParts.foldLeft(q"_root_": Tree)((l, n) => q"$l.${TermName(n)}")

    def qualifiedTypeName = tq"$qualifierTree.${TypeName(name)}"
    def qualifiedTermName = q"$qualifierTree.${TermName(name)}"

    /** The file name where this class/object must be stored */
    def toFileName: File = {
      val relPathParts = packageNameParts.map(_.toLowerCase) :+ s"${name}.scala"
      val relPath = relPathParts.mkString(File.separator)
      new File(outputDir, relPath)
    }

    def toRefTreeWithInnerTypes(innerTypes: Array[String]): RefTree = {
      qualifiedNameToRefTree(qualifiedNameParts ++ innerTypes)
    }

    def toRefTree: RefTree = toRefTreeWithInnerTypes(Array())

    override def toString: String =
      "DamlScalaName(" + packageSuffixParts.mkString("[", ", ", "]") + ", " + name + ")"

    override def equals(ojb: Any): Boolean = ojb match {
      case that: DamlScalaName =>
        that.packageSuffixParts.sameElements(this.packageSuffixParts) && that.name == this.name
      case _ =>
        false
    }

    override def hashCode(): Int = (this.packageSuffixParts.toIndexedSeq, this.name).hashCode()
  }

  /**  This method is responsible for generating Scala reflection API "tree"s.
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
        case PT.Party => q"$primitiveObject.Party"
        case PT.Text => q"$primitiveObject.Text"
        case PT.Date => q"$primitiveObject.Date"
        case PT.Timestamp => q"$primitiveObject.Timestamp"
        case PT.Unit => q"$primitiveObject.Unit"
        case PT.List | PT.ContractId | PT.Optional | PT.TextMap | PT.GenMap =>
          throw UnsupportedDamlTypeException(
            s"type $refType should not occur in a Data-kinded position; this is an invalid input LF"
          )
        case TypeConName(tyCon) =>
          val damlScalaName = mkDamlScalaName(tyCon.qualifiedName)
          damlScalaName.toRefTree
      }

    genType match {
      case TypePrim(PT.List, ImmArraySeq(typ)) =>
        val listType = genTypeToScalaType(typ)
        q"$primitiveObject.List[$listType]"
      case TypePrim(PT.ContractId, ImmArraySeq(typ)) =>
        val templateType = genTypeToScalaType(typ)
        q"$primitiveObject.ContractId[$templateType]"
      case TypePrim(PT.Optional, ImmArraySeq(typ)) =>
        val optType = genTypeToScalaType(typ)
        q"$primitiveObject.Optional[$optType]"
      case TypePrim(PT.TextMap, ImmArraySeq(typ)) =>
        val optType = genTypeToScalaType(typ)
        q"$primitiveObject.TextMap[$optType]"
      case TypePrim(PT.GenMap, ImmArraySeq(keyType, valueType)) =>
        val optKeyType = genTypeToScalaType(keyType)
        val optValueType = genTypeToScalaType(valueType)
        q"$primitiveObject.GenMap[$optKeyType, $optValueType]"
      case TypePrim(refType, ImmArraySeq()) => refTypeToIdent(refType)
      case TypeCon(name, ImmArraySeq()) => refTypeToIdent(name)
      case TypePrim(refType, typeArgs) =>
        TypeApply(refTypeToIdent(refType), typeArgs.toList map genTypeToScalaType)
      case TypeCon(name, typeArgs) =>
        TypeApply(refTypeToIdent(name), typeArgs.toList map genTypeToScalaType)
      case TypeVar(name) => toIdent(name)
      case TypeNumeric(_) => q"$primitiveObject.Numeric"
    }
  }

  def genArgsWithTypes(fields: Seq[FieldWithType]): Seq[Typed] = fields.map { case (label, typ) =>
    q"${toIdent(escapeIfReservedName(label))}: ${genTypeToScalaType(typ)}"
  }

  /** Produces the for-comprehension body of a "reader" method.
    *
    * Example: Take two variables "a: Int64", "b: Numeric", a nameOfRecord of "m"
    * This will produce:
    *
    *   if (m.fields == 2) {
    *     for {
    *       RecordField("" | "a", Some(z0)) = m.fields(0)
    *       a <- Value.decode[Int64](zv0)
    *       RecordField("" | "b", Some(zv1)) = m.fields(1)
    *       b <- Value.decode[Numeric](zv1)
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
      inner: Tree,
  ): Tree = {
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

  def genArgumentValueToGenType(genType: IType): Tree = {
    val typTree = genTypeToScalaType(genType)
    q"$domainApiAlias.Value.decode[$typTree]"
  }

  def paramRefAndGenTypeToArgumentValue(paramRef: Tree): Tree =
    q"$domainApiAlias.Value.encode($paramRef)"

  def toNamedArgumentsMap(params: List[FieldWithType], path: Option[Tree]): Tree = {
    val ps = params.map { case (n, _) =>
      val unqualified = TermName(n)
      val pr = path.cata(p => q"$p.$unqualified", q"$unqualified")
      q"($n, ${paramRefAndGenTypeToArgumentValue(pr)})"
    }
    q"` arguments`(..$ps)"
  }

  def genTemplateChoiceMethods(
      templateType: Ident,
      idType: TypeName,
      choiceId: Ref.ChoiceName,
      choiceInterface: TemplateChoice.FWT,
  ): Seq[Tree] = {
    val choiceMethod = TermName(s"exercise$choiceId")
    val choiceParam = choiceInterface.param
    val choiceParamName = toIdent("choiceArgument")
    def nonunitaryCase(
        ty: IType,
        apn: String,
        dn: Option[(Ref.QualifiedName, ImmArraySeq[FieldWithType])],
    ) =
      (
        Some(q"$choiceParamName: ${genTypeToScalaType(ty)}"),
        q"_root_.scala.Some(${paramRefAndGenTypeToArgumentValue(choiceParamName)})",
        apn,
        dn,
      )
    val (typedParam, namedArguments, actorParamName, denominalized1) =
      choiceParam match {
        case TypePrim(PT.Unit, ImmArraySeq()) => (None, q"_root_.scala.None", "actor", None)
        case TypeCon(TypeConName(tyCon), _) =>
          val dn =
            iface.typeDecls get tyCon collect {
              case TypeDecl.Normal(DefDataType(ImmArraySeq(), Record(fields))) => fields
            }
          dn map { fields =>
            val orecArgNames = fields.foldMap { case (fn, _) => Set(fn) }
            // pick a unique name for Party param among the choice params, if
            // and only if nominalized, by _-suffixing
            nonunitaryCase(
              choiceParam,
              "actor".whileDo(na => s"${na}_", orecArgNames.toSet[String]),
              Some((tyCon.qualifiedName, fields)),
            )
          } getOrElse {
            nonunitaryCase(choiceParam, "actor", None)
          }
        case ty =>
          nonunitaryCase(ty, "actor", None)
      }
    val denominalized = denominalized1.map { case (name, fields) =>
      (
        genArgsWithTypes(fields),
        mkDamlScalaName(name).qualifiedTermName,
        fields.map { case (label, _) => toIdent(escapeIfReservedName(label)) },
      )
    }
    val actorParam = q"${TermName(actorParamName)}: $domainApiAlias.Primitive.Party"
    val exerciseOnParam = q"` exOn`: $domainApiAlias.encoding.ExerciseOn[$idType, $templateType]"
    val resultType = genTypeToScalaType(choiceInterface.returnType)
    val body = q"` exercise`(id, $choiceId, $namedArguments)"

    Seq(
      q"""@deprecated("Remove the actor argument", since = "2.4.0")
            def $choiceMethod($actorParam, ..${typedParam.toList})(implicit $exerciseOnParam)
                : $domainApiAlias.Primitive.Update[$resultType] = $body""",
      q"""def $choiceMethod(..${typedParam.toList})(implicit $exerciseOnParam)
                : $domainApiAlias.Primitive.Update[$resultType] = $body""",
    ) ++
      denominalized.toList.flatMap { case (dparams, dctorName, dargs) =>
        Seq(
          q"""@deprecated("Remove the actor argument", since = "2.4.0")
                def $choiceMethod($actorParam, ..$dparams)(implicit $exerciseOnParam)
                    : $domainApiAlias.Primitive.Update[$resultType] =
                    $choiceMethod($dctorName(..$dargs))""",
          q"""def $choiceMethod(..$dparams)(implicit $exerciseOnParam)
                      : $domainApiAlias.Primitive.Update[$resultType] =
                    $choiceMethod($dctorName(..$dargs))""",
        )
      }
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
      "wait",
    ).map(Regex.quote).mkString("(?:", "|", ")_*").r.anchored
  }

  /** Either mangle a reserved name, or return it unchanged. Reserved
    * names are defined by the Scala interface section of the LF
    * specification.
    *
    * Note: '''not idempotent'''!  Escaped reserved names are ''still''
    * reserved, and will be escaped again by a second call.  See said
    * spec for details.
    */
  def escapeReservedName(name: Ref.Name): Ref.Name \/ name.type =
    name match {
      case reservedNamePrefixes(_*) => -\/(Ref.Name.assertFromString(s"${name}_"))
      case _ => \/-(name)
    }

  def escapeIfReservedName(name: Ref.Name): Ref.Name =
    escapeReservedName(name).fold(identity, identity)

  private[lf] def generateIds(number: Int, prefix: String): List[Ident] =
    List.fill(number)(prefix).zipWithIndex.map(t => toIdent(s"${t._1}${t._2}"))

  private[lf] def genConsumingChoicesMethod(templateInterface: DefTemplate[_]) = {
    val consumingChoicesIds = templateInterface.tChoices.directChoices
      .collect {
        case (id, ci) if ci.consuming => id
      }
    q"""override val consumingChoices: $stdSetType[$domainApiAlias.Primitive.ChoiceId] =
       $domainApiAlias.Primitive.ChoiceId.subst($stdSetCompanion(..$consumingChoicesIds))"""
  }

  final case class TupleNesting[A](run: NonEmptyList[A \/ TupleNesting[A]])
      extends Product
      with Serializable {
    def map[B](f: A => B): TupleNesting[B] =
      TupleNesting(run map (_.bimap(f, _ map f)))

    // not tail recursive
    def fold[Z](pure: A => Z)(roll: NonEmptyList[Z] => Z): Z =
      roll(run map (_.fold(pure, _.fold(pure)(roll))))
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
    }
    else IList.empty
    (lefts.map(\/.left[A, TupleNesting[A]])
      ++ groupedRights.map(g => \/.right(tupleNesting(g, subtrees, subtrees)))).toNel
      .map(TupleNesting(_))
      .getOrElse(
        throw new IllegalStateException(
          "impossible; either lefts or groupedRights must be non-empty"
        )
      )
  }

  private[this] def unfoldIList[S, A](init: S)(step: S => Option[(A, S)]): IList[A] =
    step(init).fold(IList.empty[A]) { case (a, next) => a :: unfoldIList(next)(step) }

  val domainApiAlias = q"` lfdomainapi`"
  val rpcValueAlias = q"` rpcvalue`"
  val rpcEventAlias = q"` rpcevent`"
  val higherKindsImport: Tree =
    q"import _root_.scala.language.higherKinds"
  val domainApiImport: Tree =
    q"import _root_.com.daml.ledger.client.{binding => ` lfdomainapi`}"
  val rpcValueImport: Tree =
    q"import _root_.com.daml.ledger.api.v1.{value => ` rpcvalue`}"
  val rpcEventImport: Tree =
    q"import _root_.com.daml.ledger.api.v1.{event => ` rpcevent`}"
  val primitiveObject: Tree = q"$domainApiAlias.Primitive"
  val stdMapType = tq"_root_.scala.collection.immutable.Map"
  val stdMapCompanion = q"_root_.scala.collection.immutable.Map"
  private val stdSetType = tq"_root_.scala.collection.immutable.Set"
  private val stdSetCompanion = q"_root_.scala.collection.immutable.Set"
  val stdVectorType = tq"_root_.scala.collection.immutable.Vector"
  val stdSeqCompanion = q"_root_.scala.collection.immutable.Seq"
  val nothingType = q"_root_.scala.Nothing"

  def toCovariantTypeDef(s: String): TypeDef = {
    val TypeDef(Modifiers(flags, mname, mtrees), name, tparams, rhs) = toTypeDef(s)
    TypeDef(Modifiers(flags | Flag.COVARIANT, mname, mtrees), name, tparams, rhs)
  }
  // error message or optional message before writing, file target, and
  // sequence of trees to write as Scala source code
  type FilePlan = String \/ (Option[String], File, Iterable[Tree])

  final case class WriteParams(
      templateIds: Map[Ref.Identifier, DefTemplateWithRecord],
      definitions: Vector[ScopedDataType.FWT],
      interfaces: Map[Ref.Identifier, typesig.DefInterface.FWT],
  )

  object WriteParams {
    def apply(tc: TransitiveClosure): WriteParams = {
      val (templateIds, typeDeclarations) = tc.serializableTypes.partitionMap { // TODO(#13349)
        case (id, TypeDecl.Template(record, template)) =>
          Left(id -> DefTemplateWithRecord(record, template))
        case (id, TypeDecl.Normal(t)) =>
          Right(ScopedDataType(id, t.typeVars, t.dataType))
      }
      WriteParams(
        templateIds = templateIds.toMap,
        definitions = typeDeclarations,
        interfaces = tc.interfaces.toMap,
      )
    }
  }

  val reservedNames: Set[String] =
    Set("id", "template", "namedArguments", "archive")

  def toNotReservedName(name: String): String =
    "userDefined" + name.capitalize

  // ----------------------------------------------

  sealed trait CodeGenDeclKind
  case object Template extends CodeGenDeclKind
  case object Contract extends CodeGenDeclKind
  case object UserDefinedType extends CodeGenDeclKind
  case object EventDecoder extends CodeGenDeclKind

  val autoGenerationHeader: String =
    """|/*
       | * THIS FILE WAS AUTOGENERATED BY THE DIGITAL ASSET Daml SCALA CODE GENERATOR
       | * DO NOT EDIT BY HAND!
       | */""".stripMargin

  def toIdent(s: String): Ident = Ident(TermName(s))

  def toTypeDef(s: String): TypeDef = q"type ${TypeName(s)}"

  def qualifiedNameToDirsAndName(qualifiedName: Ref.QualifiedName): (Array[String], String) = {
    val s = qualifiedName.module.segments.toSeq ++ qualifiedName.name.segments.toSeq
    (s.init.toArray, s.last)
  }

  def qualifiedNameToRefTree(ss: Array[String]): RefTree = {
    ss match {
      case Array() =>
        throw new RuntimeException("""|packageNameToRefTree: This function should never be called
                                      |with an empty list""".stripMargin)
      case Array(name) => toIdent(name)
      case Array(firstQual, rest @ _*) => {
        val base: RefTree = toIdent(firstQual)
        rest.foldLeft(base)((refTree: RefTree, nm: String) => {
          RefTree(refTree, TermName(nm))
        })
      }
    }
  }

  private[codegen] def packageNameTailToRefTree(packageName: String) =
    packageName
      .split('.')
      .lastOption
      .cata(TermName(_), sys error s"invalid package name $packageName")

  def packageNameToRefTree(packageName: String): RefTree = {
    val ss = packageName.split('.')
    qualifiedNameToRefTree(ss)
  }

  /** Does the type "simply delegate" (i.e. just pass along `typeVars`
    * verbatim) to another type constructor?  If so, yield that type
    * constructor as a string.
    */
  def simplyDelegates(typeVars: ImmArraySeq[Ref.Name]): IType => Option[Ref.Identifier] = {
    val ptv = typeVars.map(TypeVar(_): IType)

    {
      case TypeCon(tc, `ptv`) => Some(tc.identifier)
      case _ => None
    }
  }
}
