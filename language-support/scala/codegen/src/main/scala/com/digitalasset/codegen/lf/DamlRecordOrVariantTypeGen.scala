// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.lf

import java.io.File

import com.digitalasset.codegen.Util
import com.digitalasset.codegen.lf.LFUtil.{TupleNesting, escapeIfReservedName}
import com.digitalasset.daml.lf.iface, iface.{Type => _, _}
import com.digitalasset.daml.lf.data.Ref.{Identifier, QualifiedName}
import com.typesafe.scalalogging.Logger
import scalaz.{-\/, \/, \/-}

import scala.collection.breakOut
import scala.reflect.runtime.{universe => runUni}

/**
  *  This object is used for generating code that corresponds to a DAMLrecord or variant type
  *
  *  An app user that uses these generated classes is guaranteed to have the same level of type
  *  safety that DAML provides.
  *
  *  See the comments below for more details on what classes/methods/types are generated.
  */
object DamlRecordOrVariantTypeGen {

  import LFUtil.{domainApiAlias, generateIds, rpcValueAlias}

  import runUni._

  private val logger: Logger = Logger(getClass)

  type VariantField = (String, List[FieldWithType] \/ iface.Type)
  type RecordOrVariant = ScopedDataType.DT[FieldWithType, VariantField]

  def generate(
      util: LFUtil,
      recordOrVariant: RecordOrVariant,
      companionMembers: Iterable[Tree]): (File, Iterable[Tree]) =
    generate(
      util,
      recordOrVariant,
      isTemplate = false,
      Seq(),
      companionMembers
    )

  /**
    *  This function produces a class for a DAML type (either a record or a
    *  variant) that is defined by a `data` declaration
    */
  private[lf] def generate(
      util: LFUtil,
      typeDecl: RecordOrVariant,
      isTemplate: Boolean,
      rootClassChildren: Seq[Tree],
      companionChildren: Iterable[Tree]): (File, Iterable[Tree]) = {

    logger.debug(s"generate typeDecl: $typeDecl")

    import typeDecl.name
    val damlScalaName = util.mkDamlScalaName(Util.UserDefinedType, name)

    lazy val argumentValueProtocolDefName: TermName =
      TermName(s"${damlScalaName.name} Value")

    val typeVars: List[String] = typeDecl.typeVars.toList
    val typeVarsInUse: Set[String] = UsedTypeParams.collectTypeParamsInUse(typeDecl)
    val typeParams: List[TypeDef] = typeVars.map(LFUtil.toTypeDef)
    val typeArgs: List[TypeName] = typeVars.map(TypeName(_))
    val covariantTypeParams: List[TypeDef] = typeVars map LFUtil.toCovariantTypeDef

    val Identifier(_, QualifiedName(moduleName, baseName)) = name

    val appliedValueType: Tree =
      if (typeVars.isEmpty) damlScalaName.qualifiedTypeName
      else tq"${damlScalaName.qualifiedTypeName}[..$typeArgs]"

    val (typeParent, companionParent) =
      if (isTemplate)
        (
          tq"$domainApiAlias.Template[${TypeName(damlScalaName.name)}]",
          tq"$domainApiAlias.TemplateCompanion[${TypeName(damlScalaName.name)}]")
      else
        (tq"$domainApiAlias.ValueRef", tq"$domainApiAlias.ValueRefCompanion")

    val packageIdRef = PackageIDsGen.reference(util)(moduleName)
    val idField =
      if (isTemplate) None
      else Some(q"""
      override protected val ` recordOrVariantId` =
        ` mkRecordOrVariantId`($packageIdRef, ${moduleName.dottedName}, ${baseName.dottedName})
    """)

    /**
      *  These implicit values are used as "evidence" in the ArgumentValueFormat
      *  type class instances of polymorphic types.
      */
    val typeParamEvidences: List[Tree] = typeVars
      .filter(typeVarsInUse.contains)
      .zipWithIndex
      .map {
        case (s, ix) =>
          q"""${TermName(s"ev $ix")}: $domainApiAlias.Value[${TypeName(s)}]"""
      }

    def typeObjectMapForRecord(fields: Seq[FieldWithType], paramName: String): Tree = {
      val typeObjectContent =
        fields.map {
          case (label, typ) =>
            val reference = q"${TermName(paramName)}.${TermName(escapeIfReservedName(label))}"
            val value = util.paramRefAndGenTypeToArgumentValue(reference, typ)
            q"($label, $value)"
        }
      q"` record`(..$typeObjectContent)"
    }

    def valueTypeClassInstance(writeMethod: Tree, readMethod: Tree) = {
      val valueInstanceExpr =
        q"""
        new this.`Value ValueRef`[$appliedValueType] {
          $writeMethod
          $readMethod
        }"""
      if (typeVars.isEmpty)
        q"""
        implicit val $argumentValueProtocolDefName: $domainApiAlias.Value[$appliedValueType] =
          $valueInstanceExpr"""
      else
        q"""
        implicit def $argumentValueProtocolDefName[..$typeParams](implicit ..$typeParamEvidences): $domainApiAlias.Value[$appliedValueType] =
          $valueInstanceExpr"""
    }

    /**
      *  The generated class for a DAML variant type contains:
      *  - the definition of a "Value" trait
      *  - the definition of a _case class_ for each variant constructor of the DAML variant
      *  - "smart constructors" that create values for each cosntructor automatically up-casting
      *     to the Value (trait) type
      *  - A type class instance (i.e. implicit object) for serializing/deserializing
      *    to/from the ArgumentValue type (see typed-ledger-api project)
      */
    def toScalaDamlVariantType(fields: List[VariantField]): (Tree, Tree) = {
      lazy val damlVariant =
        if (fields.isEmpty) damlVariantZeroFields
        else damlVariantOneOrMoreFields

      /*
       *  A variant with no fields in DAML is also known as the "Void" type. It has no
       *  values. A value of this class cannot be created!
       */
      lazy val damlVariantZeroFields =
        (
          q"""
          sealed abstract class ${TypeName(damlScalaName.name)}[..$covariantTypeParams] extends $domainApiAlias.VoidValueRef {
            ..$rootClassChildren
          }""",
          q"""object ${TermName(damlScalaName.name)} extends $companionParent {
            ..${idField.toList}
            ..$companionChildren

            ${lfEncodableForVariant(fields)}
          }""")

      lazy val damlVariantOneOrMoreFields = {
        val argumentValueTypeClassInstance: Tree = valueTypeClassInstance(
          writeMethod = damlVariantArgumentValueWriteMethod,
          readMethod = damlVariantArgumentValueReadMethod)
        (
          q"""
          sealed abstract class ${TypeName(damlScalaName.name)}[..$covariantTypeParams] extends $typeParent {
            ..$rootClassChildren
          }""",
          q"""object ${TermName(damlScalaName.name)} extends $companionParent {
            ..$variantCaseClasses

            $argumentValueTypeClassInstance

            ..${idField.toList}
            ..$companionChildren

            ${lfEncodableForVariant(fields)}
          }""")
      }

      lazy val damlVariantArgumentValueWriteMethod: Tree = {
        q"""
          override def write(value: $appliedValueType): $rpcValueAlias.Value.Sum = {
            value match {
              case ..${fields.map(variantWriteCase)}
            }
          }"""
      }

      def variantWriteCase(variant: VariantField): CaseDef = variant match {
        case (label, \/-(genTyp)) =>
          cq"${TermName(label.capitalize)}(a) => ${typeObjectFromVariant(label, genTyp, Util.toIdent("a"))}"
        case (label, -\/(record)) =>
          val zs = generateIds(record.size, "z")
          val variantName = label.capitalize
          cq"${TermName(variantName)}(..$zs) => ${typeObjectFromRecordVariant(variantName, record, zs)}"
      }

      def typeObjectFromVariant(label: String, genType: iface.Type, paramName: Ident): Tree = {
        val value = util.paramRefAndGenTypeToArgumentValue(paramName, genType)
        q"` variant`($label, $value)"
      }

      def typeObjectFromRecordVariant(
          variantName: String,
          record: List[FieldWithType],
          zs: List[Ident]): Tree = {
        val tuples: List[Tree] = record.zip(zs).map {
          case ((label, genType), z) =>
            val value = util.paramRefAndGenTypeToArgumentValue(z, genType)
            q"($label, $value)"
        }
        q"` createVariantOfSynthRecord`($variantName, ..$tuples)"
      }

      lazy val damlVariantArgumentValueReadMethod: Tree = {
        val cases = fields map {
          case field @ (label, _) =>
            cq"""$label => ${variantGetBody(q"obj.value", field)}"""
        }
        q"""
           override def read(argValue: $rpcValueAlias.Value.Sum): $optionType[$appliedValueType] =
             argValue.variant.flatMap {
               obj =>
                 obj.constructor match {
                   case ..$cases
                   case _ => _root_.scala.None
                 }
             }
          """
      }

      def caseClassArg(variantType: List[FieldWithType] \/ iface.Type): Tree =
        variantType match {
          case \/-(genType) => q"body: ${util.genTypeToScalaType(genType)}"
          case -\/(record) => q"..${util.genArgsWithTypes(record)}"
        }

      lazy val variantCaseClasses: Seq[Tree] = {
        fields.map({
          case (label, typ) =>
            q"final case class ${TypeName(label)}[..$covariantTypeParams](..${caseClassArg(typ)}) extends $appliedValueType"
        })
      }

      def variantGetBody(valueExpr: Tree, field: VariantField): Tree =
        field match {
          case (label, \/-(genType)) => fieldGetBody(valueExpr, label, genType)
          case (label, -\/(record)) => recordGetBody(valueExpr, label, record)
        }

      def fieldGetBody(valueExpr: Tree, label: String, genType: iface.Type) = {
        val variantName = label.capitalize
        val variantDotApply = q"${TermName(variantName)}.apply(_)"
        val converter = util.genArgumentValueToGenType(genType)

        q"""
         $valueExpr.flatMap($converter(_)).map($variantDotApply)
         """
      }

      def recordGetBody(valueExpr: Tree, label: String, record: List[FieldWithType]) = {
        val variantName = label.capitalize
        val zs: List[Ident] = generateIds(record.size, "z")
        val decodeFields = util.genForComprehensionBodyOfReaderMethod(
          record,
          zs,
          "o2",
          q"""${TermName(variantName)}.apply(..$zs)""")
        q"$valueExpr.flatMap(_.sum.record).flatMap{o2 => $decodeFields}"
      }

      damlVariant
    }

    /**
      *  The generated class for a DAML record type contains:
      *  - the definition of a "Value" case class that contains all the DAML record fields/types.
      *  - An type class instance (i.e. implicit object) for serializing/deserializing
      *    to/from the ArgumentValue type (see typed-ledger-api project)
      */
    def toScalaDamlRecordType(fields: Seq[FieldWithType]): (Tree, Tree) = {

      lazy val damlRecord = {
        val argumentValueTypeClassInstance: Option[Tree] =
          if (isTemplate) None
          else
            Some(
              valueTypeClassInstance(
                writeMethod = damlRecordArgumentValueWriteMethod,
                readMethod = damlRecordArgumentValueReadMethod))
        val companionParentInter =
          if (typeParams.isEmpty && definitions.FunctionClass(argTypes.size) != NoSymbol)
            tq"$companionParent with ((..$argTypes) => $appliedValueType)"
          else companionParent
        (
          q"""
          final case class ${TypeName(damlScalaName.name)}[..$covariantTypeParams](..$argsWithTypes) extends $typeParent {
            ..$rootClassChildren
          }""",
          q"""object ${TermName(damlScalaName.name)} extends $companionParentInter {
             ${LFUtil.higherKindsImport}
             $viewTrait

            ..${argumentValueTypeClassInstance.toList}

            ..${idField.toList}
            ..$companionChildren

            ..${lfEncodableForRecord(fields)}
          }""")
      }

      lazy val damlRecordArgumentValueWriteMethod: Tree =
        q"""
        override def write(value: $appliedValueType): $rpcValueAlias.Value.Sum = {
          ..${typeObjectMapForRecord(fields, "value")}
        }"""

      lazy val damlRecordArgumentValueReadMethod: Tree = {
        lazy val method = q"""
          override def read(argValue: $rpcValueAlias.Value.Sum): $optionType[$appliedValueType] = {
            argValue.record flatMap {
              $typeObjectCase
            }
          }"""

        lazy val typeObjectCase = if (fields.isEmpty) {
          q"""
            {_ => _root_.scala.Some(${TermName(damlScalaName.name)}())}
          """
        } else {
          val decodeFields =
            util.genForComprehensionBodyOfReaderMethod(fields, args, " r", q"""${TermName(
              damlScalaName.name)}(..$args)""")
          q"""{` r` => $decodeFields }"""
        }

        method
      }

      lazy val viewTrait = {
        val viewMinusC =
          if (typeArgs.isEmpty) tq"view"
          else
            tq"({type ` l`[` c`[_]] = view[..$typeArgs, ` c`]})#` l`" // Lambda[c[_] => view[..., c]]
        val (viewFieldDecls, hoistFieldApps) = fields.map {
          case (label, typ) =>
            val valName = TermName(LFUtil.escapeIfReservedName(label))
            (
              q"val $valName: ` C`[${util.genTypeToScalaType(typ)}]": ValDef,
              q"override val $valName = ` f`(` view`.$valName)": ValDef)
        }.unzip
        q"""
          trait view[..$typeParams, ` C`[_]] extends $domainApiAlias.encoding.RecordView[` C`, $viewMinusC] {
            ` view` =>
            ..$viewFieldDecls
            override final def hoist[` D`[_]](` f`: _root_.scalaz.~>[` C`, ` D`])
                : view[..$typeArgs, ` D`] = new _root_.scala.AnyRef with view[..$typeArgs, ` D`] {
              ..$hoistFieldApps
            }
          }
         """
      }

      lazy val argTypes = fields map { case (_, typ) => util.genTypeToScalaType(typ) }

      lazy val argsWithTypes: Seq[Typed] = util.genArgsWithTypes(fields)
      lazy val args: Seq[Ident] = fields.map({ case (label, _) => Util.toIdent(label) })

      damlRecord
    }

    def lfEncodableForVariant(fields: Seq[VariantField]): Tree = {
      val lfEncodableName = TermName(s"${damlScalaName.name} LfEncodable")

      val variantsWithNestedRecords: Seq[(String, List[(String, iface.Type)])] =
        fields.collect { case (n, -\/(fs)) => (n, fs) }

      val recordFieldDefsByName: Seq[(String, Seq[(String, Tree)])] =
        variantsWithNestedRecords.map(a => a._1 -> generateViewFieldDefs(util)(a._2))

      val typeParamEvidences = typeVars
        .filter(typeVarsInUse.contains)
        .map(s =>
          q"""val ${TermName(s"ev$s")}: $domainApiAlias.encoding.LfEncodable[${TypeName(s)}]""")

      val viewsByName: Map[String, TermName] =
        fields.zipWithIndex.map { case ((f, _), ix) => f -> TermName(s"view $ix") }(breakOut)

      val recordFieldsByName: Map[String, TermName] =
        fields.zipWithIndex.map { case ((f, _), ix) => f -> TermName(s"recordFields $ix") }(
          breakOut)

      q"""
        implicit def $lfEncodableName[..$typeParams](implicit ..$typeParamEvidences): $domainApiAlias.encoding.LfEncodable[$appliedValueType] =
          new $domainApiAlias.encoding.LfEncodable[$appliedValueType] {
            def encoding(lte: $domainApiAlias.encoding.LfTypeEncoding): lte.Out[$appliedValueType] = {
              ..${generateViewDefList(viewsByName, recordFieldDefsByName)}
              ..${generateRecordFieldsDefList(
        typeArgs,
        viewsByName,
        recordFieldsByName,
        recordFieldDefsByName)}
              lte.variantAll(` recordOrVariantId`,
                ..${generateVariantCaseDefList(util)(
        appliedValueType,
        typeArgs,
        fields,
        recordFieldsByName)}
              )
            }
          }
      """
    }

    def lfEncodableForRecord(fields: Seq[FieldWithType]): Seq[Tree] = {
      val lfEncodableName = TermName(s"${damlScalaName.name} LfEncodable")

      val fieldDefs: Seq[(String, Tree)] = generateViewFieldDefs(util)(fields)

      val typeParamEvidences = typeVars
        .filter(typeVarsInUse.contains)
        .map(s =>
          q"""val ${TermName(s"ev$s")}: $domainApiAlias.encoding.LfEncodable[${TypeName(s)}]""")

      val view: TermName = TermName("view ")
      val recordFields: TermName = TermName("recordFields ")

      def generateEncodingBody: Tree =
        if (fields.isEmpty) {
          q"lte.emptyRecord(` recordOrVariantId`, () => $appliedValueType())"
        } else {
          q"""
            ${generateRecordFieldsDef(
            view,
            recordFields,
            appliedValueType,
            damlScalaName.qualifiedTermName,
            fieldDefs)}
            lte.record(` recordOrVariantId`, $recordFields)
          """
        }

      val viewDef = generateViewDef(view, fieldDefs, Some(tq"view[..$typeArgs, lte.Field]"))

      val encodingDependentMethod =
        q"""
          override def encoding(lte: $domainApiAlias.encoding.LfTypeEncoding
                              )(...${if (isTemplate) Seq(q"$view: view[lte.Field]") else Seq.empty}
                              ): lte.Out[$appliedValueType] = {
            ..${if (isTemplate) Seq.empty else Seq(viewDef)}
            $generateEncodingBody
          }
         """

      if (isTemplate)
        Seq(
          q"""
          override def fieldEncoding(lte: $domainApiAlias.encoding.LfTypeEncoding): view[lte.Field] = {
            $viewDef
            $view
          }
         """,
          encodingDependentMethod
        )
      else
        Seq(q"""
          implicit def $lfEncodableName[..$typeParams](implicit ..$typeParamEvidences): $domainApiAlias.encoding.LfEncodable[$appliedValueType] =
            new $domainApiAlias.encoding.LfEncodable[$appliedValueType] {
              $encodingDependentMethod
            }
        """)
    }

    val (klass, companion) = typeDecl.dataType match {
      case Record(fields) => toScalaDamlRecordType(fields)
      case Variant(fields) => toScalaDamlVariantType(fields.toList)
    }

    val filePath = damlScalaName.toFileName
    (filePath, Seq(klass, companion))
  }

  private def generateViewDef(
      view: TermName,
      fields: Seq[(String, Tree)],
      extend: Option[Tree] = None): Tree = {
    val viewFields = fields.map {
      case (name, definition) => q"val ${TermName(name)} = $definition"
    }
    q"object $view extends ${extend getOrElse tq"_root_.scala.AnyRef"} { ..$viewFields }"
  }

  private def generateViewDefList(
      viewsByName: Map[String, TermName],
      fieldDefsByName: Seq[(String, Seq[(String, Tree)])]): Seq[Tree] =
    fieldDefsByName.map { case (n, fs) => generateViewDef(viewsByName(n), fs) }

  private def generateRecordFieldsDef(
      view: TermName,
      recordFields: TermName,
      appliedValueType: Tree,
      constructor: Tree,
      fields: Seq[(String, Tree)]): Tree = {
    val names: Seq[TermName] = fields.map(x => TermName(x._1))
    val tupledNames: Option[TupleNesting[TermName]] = tupleUp(names)
    val shapedTuple: Tree = shapeTuple(tupledNames).getOrElse(emptyTuple)

    val tupledViewFields: Option[TupleNesting[Tree]] =
      tupledNames.map(_.map(x => q"lte.fields($view.$x)"))
    val shapedRecord: Seq[Tree] = tupledViewFields
      .map(tpls => tpls.run.map(shapeRecord).list.toList)
      .getOrElse(List(emptyTuple))

    q"""
        val $recordFields: lte.RecordFields[$appliedValueType] = lte.RecordFields.xmapN(..$shapedRecord) {
          case (..$shapedTuple) => $appliedValueType(..$names)
        } {
          case $constructor(..$names) => (..$shapedTuple)
        }
      """
  }

  private def generateRecordFieldsDefList(
      typeArgs: List[TypeName],
      viewsByName: Map[String, TermName],
      recordFieldsByName: Map[String, TermName],
      fieldDefsByName: Seq[(String, Seq[(String, Tree)])]): Seq[Tree] =
    fieldDefsByName.map {
      case (n, fs) =>
        val constructor = q"${TermName(n)}"
        generateRecordFieldsDef(
          viewsByName(n),
          recordFieldsByName(n),
          q"$constructor[..$typeArgs]",
          constructor,
          fs)
    }

  private def generateViewFieldDefs(util: LFUtil)(fields: Seq[FieldWithType]): Seq[(String, Tree)] =
    fields.map {
      case (label, typ) =>
        (escapeIfReservedName(label), generateViewFieldDef(util)(label, typ))
    }

  private def generateViewFieldDef(util: LFUtil)(name: String, typ: iface.Type): Tree =
    q"""lte.field($name, ${generateEncodingCall(util)(typ)})"""

  private def generateEncodingCall(util: LFUtil)(typ: iface.Type): Tree = {
    val tree: Tree = util.genTypeToScalaType(typ)
    q"""$domainApiAlias.encoding.LfEncodable.encoding[$tree](lte)"""
  }

  private val RootMax = 5
  private val SubtreesMax = 5

  private def tupleUp[A](fieldNames: Seq[A]): Option[TupleNesting[A]] = {
    import scalaz.IList
    IList(fieldNames: _*).toNel.map(xs => LFUtil.tupleNesting(xs, RootMax, SubtreesMax))
  }

  private def shapeTuple(ns: TupleNesting[TermName]): Tree =
    ns.fold(x => q"(..$x)")(x => q"(..${x.list.toList})")

  private def shapeTuple(o: Option[TupleNesting[TermName]]): Option[Tree] =
    o.map(shapeTuple)

  private def shapeRecord(xs: TupleNesting[Tree]): Tree =
    xs.fold(x => q"(..$x)")(x => q"lte.RecordFields.tupleN(..${x.list.toList})")

  private def shapeRecord(a: Tree \/ TupleNesting[Tree]): Tree = a match {
    case -\/(t) => t
    case \/-(ts) => shapeRecord(ts)
  }

  private def generateVariantCaseDefList(util: LFUtil)(
      appliedValueType: Tree,
      typeArgs: List[TypeName],
      fields: Seq[VariantField],
      recordFieldsByName: Map[String, TermName]): Seq[Tree] =
    fields.map {
      case (n, -\/(r)) =>
        generateVariantRecordCase(appliedValueType, typeArgs, n, recordFieldsByName(n), r)
      case (n, \/-(v)) =>
        generateVariantCase(util)(appliedValueType, n, v)
    }

  private def generateVariantRecordCase(
      appliedValueType: Tree,
      typeArgs: List[TypeName],
      caseName: String,
      recordFieldsName: TermName,
      fields: List[FieldWithType]): Tree = {
    val placeHolders: Seq[TermName] = Seq.fill(fields.size)(TermName("_"))
    val variantType: Tree = q"${TermName(caseName)}[..$typeArgs]"
    q"""
      lte.variantRecordCase[$variantType, $appliedValueType]($caseName,
        ` recordOrVariantId`, $recordFieldsName) {
        case x @ ${TermName(caseName)}(..$placeHolders) =>  x
      }
    """
  }

  private def generateVariantCase(
      util: LFUtil)(appliedValueType: Tree, caseName: String, typ: iface.Type): Tree = {
    val variant: TermName = TermName(caseName)
    val variantTypes: Tree = util.genTypeToScalaType(typ)
    q"""
      lte.variantCase[$variantTypes, $appliedValueType]($caseName,
        ${generateEncodingCall(util)(typ)}
        ) {
          x => $variant(x)
        } {
          case $variant(x) => x
        }
    """
  }

  private[this] val optionType: Tree = tq"_root_.scala.Option"
  private[this] val emptyTuple: Tree = q"()"
}
