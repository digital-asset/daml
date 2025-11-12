// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast

private[codegen] sealed trait DefGen {
  def renderJsSource: String
  def renderTsExport: String
}

private[codegen] final case class NamespaceGen(name: Name, definitions: Seq[TypeConGen])
    extends DefGen {
  override def renderJsSource: String = ""

  override def renderTsExport: String =
    s"""|export namespace $name {
        |  ${definitions.map(_.renderTsDecl(indent = "  ")).mkString("\n  ")}
        |} // namespace""".stripMargin
}

private[codegen] final case class TemplateGen(
    moduleId: ModuleId,
    packageName: PackageName,
    name: Name,
    decoder: Decoder,
    encode: Encode,
    keyDecoderOpt: Option[Decoder],
    keyEncode: Encode,
    choices: Seq[ChoiceGen],
    implements: Seq[TypeConId],
) extends DefGen {
  private val templateId = s"#$packageName:${moduleId.moduleName}:$name"
  override def renderJsSource: String = {
    val keyDecoder =
      keyDecoderOpt.map(LazyDecoder(_)).getOrElse(ConstantRefDecoder(Seq("undefined")))
    s"""|exports.$name = damlTypes.assembleTemplate(
        |  {
        |    templateId: '$templateId',
        |    keyDecoder: ${keyDecoder.render(moduleId)},
        |    keyEncode: ${keyEncode.render(moduleId, indent = "    ")},
        |    decoder: ${LazyDecoder(decoder).render(moduleId)},
        |    encode: ${encode.render(moduleId, indent = "    ")}
        |    ${choices.map(_.renderJsSource(moduleId, name, indent = "    ")).mkString("\n    ")}
        |  },
        |  ${implements.map(TypeGen.renderSerializable(moduleId, _)).mkString(",\n  ")} 
        |);""".stripMargin
  }

  override def renderTsExport: String = {
    val renderKeyType = keyDecoderOpt.map(_ => s"$name.Key").getOrElse("undefined")
    val renderImplsUnion =
      if (implements.nonEmpty)
        implements.map(TypeGen.renderType(moduleId, _)).mkString(" | ")
      else "never"
    s"""|export declare interface ${name}Interface {
        |  ${choices
        .map(_.renderTsDeclFromTemplate(moduleId, name, renderKeyType, indent = "  "))
        .mkString("\n  ")}
        |}
        |
        |export declare const $name:
        |  damlTypes.Template<$name, $renderKeyType, '$templateId'> &
        |  damlTypes.ToInterface<$name, $renderImplsUnion> &
        |  ${name}Interface
        |""".stripMargin
  }
}

private[codegen] final case class TemplateNamespaceGen(
    moduleId: ModuleId,
    name: Name,
    key: Ast.Type,
) extends DefGen {
  override def renderJsSource: String = ""
  override def renderTsExport: String =
    s"""|export declare namespace $name {
        |  export type Key = ${TypeGen.renderType(moduleId, key)}
        |}
        |""".stripMargin
}

private[codegen] final case class TemplateRegistrationGen(
    packageId: PackageId,
    packageName: PackageName,
    name: Name,
) extends DefGen {
  override def renderJsSource: String =
    s"damlTypes.registerTemplate(exports.$name, ['$packageId', '#$packageName']);"

  override def renderTsExport: String = ""
}

private[codegen] final case class TypeConGen(
    moduleId: ModuleId,
    name: Name,
    paramNames: Seq[Name],
    cons: Ast.DataCons,
) extends DefGen {
  override def renderJsSource: String = ""

  override def renderTsExport: String = s"export declare ${renderTsDecl(indent = "")}"

  def renderTsDecl(indent: String): String = {
    val typeDecl =
      if (paramNames.sizeIs == 0) s"type $name"
      else s"type $name<${paramNames.mkString(", ")}>"
    cons match {
      case Ast.DataRecord(fields) =>
        val renderFields = fields.toSeq
          .map { case (name, tpe) => s"$name: ${TypeGen.renderType(moduleId, tpe)};" }
        s"""|$indent$typeDecl = {
            |$indent  ${renderFields.mkString(s"\n$indent  ")}
            |$indent};""".stripMargin
      case Ast.DataVariant(variants) =>
        val renderVariants = variants.toSeq
          .map { case (name, tpe) =>
            s"| { tag: '$name', value: ${TypeGen.renderType(moduleId, tpe)} }"
          }
        s"""|$indent$typeDecl =
            |$indent  ${renderVariants.mkString(s"\n$indent  ")}
            |$indent;""".stripMargin
      case Ast.DataEnum(constructors) =>
        val renderConstructors = constructors.toSeq.map(name => s"| '$name'")
        s"""|$indent$typeDecl =
            |$indent  ${renderConstructors.mkString(s"\n$indent  ")}
            |$indent;""".stripMargin
      case Ast.DataInterface => throw new RuntimeException("interfaces are not serializable")
    }
  }
}

private[codegen] final case class SerializableGen(
    moduleId: ModuleId,
    name: ChoiceName,
    paramNames: Seq[Name],
    keys: Seq[Name],
    decoder: Decoder,
    encode: Encode,
    nestedSerializable: Seq[NestedSerializable],
) extends DefGen {
  override def renderJsSource: String = {
    val renderKeys =
      if (keys.nonEmpty)
        s"""|  ${keys.map(k => s"$k: '$k',").mkString("\n  ")}
            |  keys: [${keys.map(k => s"'$k'").mkString(", ")}],""".stripMargin
      else ""
    val renderNested = nestedSerializable.map(_.renderJsSource(moduleId, indent = "  "))
    s"""|exports.$name = {
        |  $renderKeys
        |  decoder: ${LazyDecoder(decoder).render(moduleId)},
        |  encode: ${encode.render(moduleId, indent = "  ")},
        |  ${renderNested.mkString("\n  ")}
        |}""".stripMargin
  }

  override def renderTsExport: String = {
    val renderKeys =
      if (keys.nonEmpty) s" & { readonly keys: $name[] } & { readonly [e in $name]: e }"
      else ""
    s"""|export declare const $name:
        |  damlTypes.Serializable<$name> & {
        |    ${nestedSerializable.map(_.renderTsDecl).mkString("\n    ")}
        |  }$renderKeys;
        |""".stripMargin
  }
}

private[codegen] final case class NestedSerializable(
    dottedName: DottedName,
    decoder: Decoder,
    encode: Encode,
) {
  private val name = dottedName.segments.last
  def renderJsSource(moduleId: ModuleId, indent: String): String =
    s"""|$name: {
        |$indent  decoder: ${LazyDecoder(decoder).render(moduleId)},
        |$indent  encode: ${encode.render(moduleId, indent = "  ")},
        |$indent},
        |""".stripMargin

  def renderTsDecl: String = s"$name: damlTypes.Serializable<$dottedName>;"
}

private[codegen] final case class InterfaceGen(
    moduleId: ModuleId,
    packageName: PackageName,
    name: Name,
    choices: Seq[ChoiceGen],
    view: TypeConId,
) extends DefGen {
  private val interfaceId = s"#$packageName:${moduleId.moduleName}:$name"
  override def renderJsSource: String =
    s"""|exports.$name = damlTypes.assembleInterface(
        |  '$interfaceId',
        |  function () { return ${TypeGen.renderSerializable(moduleId, view)}; },
        |  {
        |    ${choices.map(_.renderJsSource(moduleId, name, indent = "    ")).mkString("\n    ")}
        |  }
        |);
        |""".stripMargin

  override def renderTsExport: String =
    s"""|export declare type $name = damlTypes.Interface<'$interfaceId'> & ${TypeGen.renderType(
        moduleId,
        view,
      )}
        |
        |export declare interface ${name}Interface {
        |  ${choices
        .map(_.renderTsDeclFromInterface(moduleId, name, indent = "  "))
        .mkString("\n  ")}
        |}
        |
        |""".stripMargin
}

private[codegen] final case class ChoiceGen(name: Name, argType: Ast.Type, returnType: Ast.Type) {
  def renderJsSource(
      currentModule: ModuleId,
      templateOrInterfaceName: Name,
      indent: String,
  ): String =
    s"""|$name: {
        |$indent  template: function () { return exports.${templateOrInterfaceName}; },
        |$indent  choiceName: $name,
        |$indent  argumentDecoder: ${LazyDecoder(TypeDecoder(argType)).render(currentModule)},
        |$indent  argumentEncode: ${TypeEncode(argType).render(currentModule, indent + "  ")},
        |$indent  resultDecoder: ${LazyDecoder(TypeDecoder(returnType)).render(currentModule)},
        |$indent  resultEncode: ${TypeEncode(returnType).render(currentModule, indent + "  ")},
        |$indent},""".stripMargin

  def renderTsDeclFromTemplate(
      currentModule: ModuleId,
      templateName: Name,
      keyType: String,
      indent: String,
  ): String =
    s"""|$name: damlTypes.Choice<
        |$indent  $templateName,
        |$indent  ${TypeGen.renderType(currentModule, argType)},
        |$indent  ${TypeGen.renderType(currentModule, returnType)},
        |$indent  $keyType & damlTypes.ChoiceFrom<damlTypes.Template<$templateName, $keyType>>;""".stripMargin

  def renderTsDeclFromInterface(
      currentModule: ModuleId,
      interfaceName: Name,
      indent: String,
  ): String =
    s"""|$name: damlTypes.Choice<
        |$indent  $interfaceName,
        |$indent  ${TypeGen.renderType(currentModule, argType)},
        |$indent  ${TypeGen.renderType(currentModule, returnType)},
        |$indent  undefined & damlTypes.ChoiceFrom<damlTypes.InterfaceCompanion<$interfaceName, undefined>>;""".stripMargin
}
