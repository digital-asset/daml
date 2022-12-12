// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.codegen.lf.LFUtil.{TupleNesting, escapeIfReservedName}
import UsedTypeParams.Variance.{Covariant, Invariant}
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import com.typesafe.scalalogging.Logger
import scalaz.{-\/, \/, \/-}

import scala.reflect.runtime.{universe => runUni}

/**  This object is used for generating code that corresponds to a Daml record or variant type
  *
  *  An app user that uses these generated classes is guaranteed to have the same level of type
  *  safety that Daml provides.
  *
  *  See the comments below for more details on what classes/methods/types are generated.
  */
object DamlDataTypeGen {

  import LFUtil.{domainApiAlias, generateIds, rpcValueAlias, stdVectorType}

  import runUni._

  private val logger: Logger = Logger(getClass)

  type FieldWithType = (Ref.Name, typesig.Type)
  type VariantField = List[FieldWithType] \/ typesig.Type
  type DataType = ScopedDataType.DT[typesig.Type, VariantField]

  def generate(
      util: LFUtil,
      recordOrVariant: DataType,
      companionMembers: Iterable[Tree],
  ): (File, Set[Tree], Iterable[Tree]) =
    generate(
      util,
      recordOrVariant,
      isTemplate = false,
      Seq(),
      companionMembers,
    )

  /**  This function produces a class for a Daml type (either a record or a
    *  variant) that is defined by a `data` declaration
    */
  private[lf] def generate(
      util: LFUtil,
      typeDecl: DataType,
      isTemplate: Boolean,
      rootClassChildren: Seq[Tree],
      companionChildren: Iterable[Tree],
  ): (File, Set[Tree], Iterable[Tree]) = {

    logger.debug(s"generate typeDecl: $typeDecl")

    import typeDecl.name
    val damlScalaName = util.mkDamlScalaName(name.qualifiedName)

    lazy val argumentValueProtocolDefName: TermName =
      TermName(s"${damlScalaName.name} Value")

    val variance = new VarianceCache(util.iface)

    val typeVars: List[String] = typeDecl.typeVars.toList
    val typeVarsInUse: Set[String] = UsedTypeParams.collectTypeParamsInUse(typeDecl)
    val typeParams: List[TypeDef] = typeVars.map(LFUtil.toTypeDef)
    val typeArgs: List[TypeName] = typeVars.map(TypeName(_))
    val covariantTypeParams: List[TypeDef] = (typeVars zip variance(typeDecl)) map {
      case (v, Covariant) => LFUtil.toCovariantTypeDef(v)
      case (v, Invariant) => LFUtil.toTypeDef(v)
    }

    val Ref.Identifier(_, Ref.QualifiedName(moduleName, baseName)) = name

    val appliedValueType: Tree =
      if (typeVars.isEmpty) damlScalaName.qualifiedTypeName
      else tq"${damlScalaName.qualifiedTypeName}[..$typeArgs]"

    val (typeParent, companionParent) =
      if (isTemplate)
        (
          tq"$domainApiAlias.Template[${TypeName(damlScalaName.name)}]",
          tq"$domainApiAlias.TemplateCompanion[${TypeName(damlScalaName.name)}]",
        )
      else
        (tq"$domainApiAlias.ValueRef", tq"$domainApiAlias.ValueRefCompanion")

    val packageIdRef = PackageIDsGen.reference(moduleName)
    val idField =
      if (isTemplate) None
      else Some(q"""
      override protected val ` dataTypeId` =
        ` mkDataTypeId`($packageIdRef, ${moduleName.dottedName}, ${baseName.dottedName})
    """)

    // These implicit values are used as "evidence" in the ArgumentValueFormat
    // type class instances of polymorphic types.
    val typeParamEvidences: List[Tree] = typeVars
      .filter(typeVarsInUse.contains)
      .zipWithIndex
      .map { case (s, ix) =>
        q"""${TermName(s"ev $ix")}: $domainApiAlias.Value[${TypeName(s)}]"""
      }

    def typeObjectMapForRecord(fields: Seq[FieldWithType], paramName: String): Tree = {
      val typeObjectContent =
        fields.map { case (label, _) =>
          val reference = q"${TermName(paramName)}.${TermName(escapeIfReservedName(label))}"
          val value = util.paramRefAndGenTypeToArgumentValue(reference)
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

    // The generated class for a Daml enum type contains:
    //  - the definition of a "Value" trait
    //  - the definition of a _case object_ for each constructor of the Daml enum
    //  - A type class instance (i.e. implicit object) for serializing/deserializing
    //    to/from the ArgumentValue type (see typed-ledger-api project)
    def toScalaDamlEnumType(constructors: List[Ref.Name]): (Set[Tree], (Tree, Tree)) = {
      val className = damlScalaName.name.capitalize

      val klass =
        q"""
          sealed abstract class ${TypeName(className)}(
            override val constructor: String,
            override val index: Int
          ) extends ${tq"$domainApiAlias.EnumRef"} {
            ..$rootClassChildren
          }"""

      val (imports, companionObject) = (constructors: List[Ref.Name]) match {
        case firstValue :: otherValues =>
          Set(LFUtil.domainApiImport) ->
            q"""
            object ${TermName(className)} extends
              ${tq"$domainApiAlias.EnumCompanion[$appliedValueType]"} {
              ..${constructors.zipWithIndex.map { case (c, i) =>
                q"""case object ${TermName(c.capitalize)} extends $appliedValueType($c, $i) """
              }}
            
              val firstValue: $appliedValueType = ${TermName(firstValue.capitalize)}
              val otherValues:  $stdVectorType[$appliedValueType] =
                ${otherValues.map(c => q"${TermName(c.capitalize)}").toVector}

              ..${idField.toList}
              ..$companionChildren  
            }"""
        case _ =>
          throw new IllegalAccessException("empty Enum not allowed")
      }

      (imports, (klass, companionObject))

    }

    // The generated class for a Daml variant type contains:
    // - the definition of a "Value" trait
    // - the definition of a _case class_ for each variant constructor of the Daml variant
    // - "smart constructors" that create values for each constructor automatically up-casting
    //    to the Value (trait) type
    // - A type class instance (i.e. implicit object) for serializing/deserializing
    //   to/from the ArgumentValue type (see typed-ledger-api project)
    def toScalaDamlVariantType(fields: List[(Ref.Name, VariantField)]): (Tree, Tree) = {
      lazy val damlVariant = {
        val (variantParent, argumentValueTypeClassInstance) =
          if (fields.isEmpty) (tq"$domainApiAlias.VoidValueRef", None)
          else
            (
              typeParent,
              Some(
                valueTypeClassInstance(
                  writeMethod = damlVariantArgumentValueWriteMethod,
                  readMethod = damlVariantArgumentValueReadMethod,
                )
              ),
            )
        (
          q"""
          sealed abstract class ${TypeName(
              damlScalaName.name
            )}[..$covariantTypeParams] extends $variantParent {
            ..$rootClassChildren
          }""",
          q"""object ${TermName(damlScalaName.name)} extends $companionParent {
            ..$variantCaseClasses

            ..${argumentValueTypeClassInstance.toList}

            ..${idField.toList}
            ..$companionChildren

            ${lfEncodableForVariant(fields)}
          }""",
        )
      }

      lazy val damlVariantArgumentValueWriteMethod: Tree = {
        q"""
          override def write(value: $appliedValueType): $rpcValueAlias.Value.Sum = {
            value match {
              case ..${fields.map(variantWriteCase)}
            }
          }"""
      }

      def variantWriteCase(variant: (Ref.Name, VariantField)): CaseDef = variant match {
        case (label, \/-(_)) =>
          cq"${TermName(label.capitalize)}(a) => ${typeObjectFromVariant(label, LFUtil.toIdent("a"))}"
        case (label, -\/(record)) =>
          val zs = generateIds(record.size, "z")
          val variantName = label.capitalize
          cq"${TermName(variantName)}(..$zs) => ${typeObjectFromRecordVariant(variantName, record, zs)}"
      }

      def typeObjectFromVariant(label: String, paramName: Ident): Tree = {
        val value = util.paramRefAndGenTypeToArgumentValue(paramName)
        q"` variant`($label, $value)"
      }

      def typeObjectFromRecordVariant(
          variantName: String,
          record: List[FieldWithType],
          zs: List[Ident],
      ): Tree = {
        val tuples: List[Tree] = record.zip(zs).map { case ((label, _), z) =>
          val value = util.paramRefAndGenTypeToArgumentValue(z)
          q"($label, $value)"
        }
        q"` createVariantOfSynthRecord`($variantName, ..$tuples)"
      }

      lazy val damlVariantArgumentValueReadMethod: Tree = {
        val cases = fields map { case field @ (label, _) =>
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

      def caseClassArg(variantType: List[FieldWithType] \/ typesig.Type): Tree =
        variantType match {
          case \/-(genType) => q"body: ${util.genTypeToScalaType(genType)}"
          case -\/(record) => q"..${util.genArgsWithTypes(record)}"
        }

      lazy val variantCaseClasses: Seq[Tree] = {
        fields.map({ case (label, typ) =>
          q"final case class ${TypeName(label)}[..$covariantTypeParams](..${caseClassArg(typ)}) extends $appliedValueType"
        })
      }

      def variantGetBody(valueExpr: Tree, field: (String, VariantField)): Tree =
        field match {
          case (label, \/-(genType)) => fieldGetBody(valueExpr, label, genType)
          case (label, -\/(record)) => recordGetBody(valueExpr, label, record)
        }

      def fieldGetBody(valueExpr: Tree, label: String, genType: typesig.Type) = {
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
          q"""${TermName(variantName)}.apply(..$zs)""",
        )
        q"$valueExpr.flatMap(_.sum.record).flatMap{o2 => $decodeFields}"
      }

      damlVariant
    }

    // The generated class for a Daml record type contains:
    // - the definition of a "Value" case class that contains all the Daml record fields/types.
    // - An type class instance (i.e. implicit object) for serializing/deserializing
    //   to/from the ArgumentValue type (see typed-ledger-api project)
    def toScalaDamlRecordType(fields: Seq[FieldWithType]): (Tree, Tree) = {

      lazy val damlRecord = {
        val argumentValueTypeClassInstance: Option[Tree] =
          if (isTemplate) None
          else
            Some(
              valueTypeClassInstance(
                writeMethod = damlRecordArgumentValueWriteMethod,
                readMethod = damlRecordArgumentValueReadMethod,
              )
            )
        val companionParentInter =
          if (typeParams.isEmpty && definitions.FunctionClass(argTypes.size) != NoSymbol)
            tq"$companionParent with ((..$argTypes) => $appliedValueType)"
          else companionParent
        (
          q"""
          final case class ${TypeName(
              damlScalaName.name
            )}[..$covariantTypeParams](..$argsWithTypes) extends $typeParent {
            ..$rootClassChildren
          }""",
          q"""object ${TermName(damlScalaName.name)} extends $companionParentInter {
             ${LFUtil.higherKindsImport}
             $viewTrait

            ..${argumentValueTypeClassInstance.toList}

            ..${idField.toList}
            ..$companionChildren

            ..${lfEncodableForRecord(fields)}
          }""",
        )
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
            {_root_.scala.Function const _root_.scala.Some(${TermName(damlScalaName.name)}())}
          """
        } else {
          val decodeFields =
            util.genForComprehensionBodyOfReaderMethod(
              fields,
              args,
              " r",
              q"""${TermName(damlScalaName.name)}(..$args)""",
            )
          q"""{` r` => $decodeFields }"""
        }

        method
      }

      lazy val viewTrait = {
        val viewMinusC =
          if (typeArgs.isEmpty) tq"view"
          else
            tq"({type ` l`[` c`[_]] = view[..$typeArgs, ` c`]})#` l`" // Lambda[c[_] => view[..., c]]
        val (viewFieldDecls, hoistFieldApps) = fields.map { case (label, typ) =>
          val valName = TermName(LFUtil.escapeIfReservedName(label))
          (
            q"val $valName: ` C`[${util.genTypeToScalaType(typ)}]": ValDef,
            q"override val $valName = ` f`(` view`.$valName)": ValDef,
          )
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
      lazy val args: Seq[Ident] = fields.map({ case (label, _) => LFUtil.toIdent(label) })

      damlRecord
    }

    def lfEncodableForVariant(fields: Seq[(Ref.Name, VariantField)]): Tree = {
      val lfEncodableName = TermName(s"${damlScalaName.name} LfEncodable")

      val variantsWithNestedRecords: Seq[(Ref.Name, List[(Ref.Name, typesig.Type)])] =
        fields.collect { case (n, -\/(fs)) => (n, fs) }

      val recordFieldDefsByName: Seq[(Ref.Name, Seq[(Ref.Name, Tree)])] =
        variantsWithNestedRecords.map(a => a._1 -> generateViewFieldDefs(util)(a._2))

      val typeParamEvidences = typeVars
        .filter(typeVarsInUse.contains)
        .map(s =>
          q"""val ${TermName(s"ev$s")}: $domainApiAlias.encoding.LfEncodable[${TypeName(s)}]"""
        )

      val viewsByName: Map[String, TermName] =
        fields.zipWithIndex.view.map { case ((f, _), ix) => f -> TermName(s"view $ix") }.toMap

      val recordFieldsByName: Map[String, TermName] =
        fields.zipWithIndex.view.map { case ((f, _), ix) =>
          f -> TermName(s"recordFields $ix")
        }.toMap

      q"""
        implicit def $lfEncodableName[..$typeParams](implicit ..$typeParamEvidences): $domainApiAlias.encoding.LfEncodable[$appliedValueType] =
          new $domainApiAlias.encoding.LfEncodable[$appliedValueType] {
            def encoding(lte: $domainApiAlias.encoding.LfTypeEncoding): lte.Out[$appliedValueType] = {
              ..${generateViewDefList(viewsByName, recordFieldDefsByName)}
              ..${generateRecordFieldsDefList(
          typeArgs,
          viewsByName,
          recordFieldsByName,
          recordFieldDefsByName,
        )}
              lte.variantAll(` dataTypeId`,
                ..${generateVariantCaseDefList(util)(
          appliedValueType,
          typeArgs,
          fields,
          recordFieldsByName,
        )}
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
          q"""val ${TermName(s"ev$s")}: $domainApiAlias.encoding.LfEncodable[${TypeName(s)}]"""
        )

      val view: TermName = TermName("view ")
      val recordFields: TermName = TermName("recordFields ")

      def generateEncodingBody: Tree =
        if (fields.isEmpty) {
          q"lte.emptyRecord(` dataTypeId`, () => $appliedValueType())"
        } else {
          q"""
            ${generateRecordFieldsDef(
              view,
              recordFields,
              appliedValueType,
              damlScalaName.qualifiedTermName,
              fieldDefs,
            )}
            lte.record(` dataTypeId`, $recordFields)
          """
        }

      val viewDef = generateViewDef(view, fieldDefs, Some(tq"view[..$typeArgs, lte.Field]"))

      val encodingDependentMethod =
        q"""
          override def encoding(lte: $domainApiAlias.encoding.LfTypeEncoding
                              )(...${if (isTemplate) Seq(q"$view: view[lte.Field]") else Seq.empty}
                              ): lte.Out[$appliedValueType] = {
            ..${if (isTemplate || fieldDefs.isEmpty) Seq.empty else Seq(viewDef)}
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
          encodingDependentMethod,
        )
      else
        Seq(q"""
          implicit def $lfEncodableName[..$typeParams](implicit ..$typeParamEvidences): $domainApiAlias.encoding.LfEncodable[$appliedValueType] =
            new $domainApiAlias.encoding.LfEncodable[$appliedValueType] {
              $encodingDependentMethod
            }
        """)
    }

    val (imports, (klass, companion)) = typeDecl.dataType match {
      case typesig.Record(fields) => defaultImports -> toScalaDamlRecordType(fields)
      case typesig.Variant(fields) => defaultImports -> toScalaDamlVariantType(fields.toList)
      case typesig.Enum(values) => toScalaDamlEnumType(values.toList)
    }

    val filePath = damlScalaName.toFileName
    (filePath, imports, Seq(klass, companion))
  }

  private def generateViewDef(
      view: TermName,
      fields: Seq[(String, Tree)],
      extend: Option[Tree] = None,
  ): Tree = {
    val viewFields = fields.map { case (name, definition) =>
      q"val ${TermName(name)} = $definition"
    }
    q"object $view extends ${extend getOrElse tq"_root_.scala.AnyRef"} { ..$viewFields }"
  }

  private def generateViewDefList(
      viewsByName: Map[String, TermName],
      fieldDefsByName: Seq[(String, Seq[(String, Tree)])],
  ): Seq[Tree] =
    fieldDefsByName.map { case (n, fs) => generateViewDef(viewsByName(n), fs) }

  private def generateRecordFieldsDef(
      view: TermName,
      recordFields: TermName,
      appliedValueType: Tree,
      constructor: Tree,
      fields: Seq[(String, Tree)],
  ): Tree = {
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
      fieldDefsByName: Seq[(String, Seq[(String, Tree)])],
  ): Seq[Tree] =
    fieldDefsByName.map { case (n, fs) =>
      val constructor = q"${TermName(n)}"
      generateRecordFieldsDef(
        viewsByName(n),
        recordFieldsByName(n),
        q"$constructor[..$typeArgs]",
        constructor,
        fs,
      )
    }

  private def generateViewFieldDefs(util: LFUtil)(
      fields: Seq[FieldWithType]
  ): Seq[(Ref.Name, Tree)] =
    fields.map { case (label, typ) =>
      (escapeIfReservedName(label), generateViewFieldDef(util)(label, typ))
    }

  private def generateViewFieldDef(util: LFUtil)(name: Ref.Name, typ: typesig.Type): Tree =
    q"""lte.field($name, ${generateEncodingCall(util)(typ)})"""

  private def generateEncodingCall(util: LFUtil)(typ: typesig.Type): Tree = {
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
      fields: Seq[(String, VariantField)],
      recordFieldsByName: Map[String, TermName],
  ): Seq[Tree] =
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
      fields: List[FieldWithType],
  ): Tree = {
    val placeHolders: Seq[TermName] = Seq.fill(fields.size)(TermName("_"))
    val variantType: Tree = q"${TermName(caseName)}[..$typeArgs]"
    q"""
      lte.variantRecordCase[$variantType, $appliedValueType]($caseName,
        ` dataTypeId`, $recordFieldsName) {
        case x @ ${TermName(caseName)}(..$placeHolders) =>  x
      }
    """
  }

  private def generateVariantCase(
      util: LFUtil
  )(appliedValueType: Tree, caseName: String, typ: typesig.Type): Tree = {
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

  private[this] val defaultImports = Set(LFUtil.domainApiImport, LFUtil.rpcValueImport)
}
