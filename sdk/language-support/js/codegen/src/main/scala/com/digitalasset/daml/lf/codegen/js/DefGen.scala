// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast

private[codegen] sealed trait DefGen {
  def renderJsSource(b: CodeBuilder): Unit
  def renderTsExport(b: CodeBuilder): Unit
}

private[codegen] final case class NamespaceGen(name: Name, definitions: Seq[TypeConGen])
    extends DefGen {
  override def renderJsSource(b: CodeBuilder): Unit = ()

  override def renderTsExport(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    b.addBlock(s"export namespace $name {", "}") {
      definitions.foreach(_.renderTsDecl(b))
    }
  }
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
  override def renderJsSource(b: CodeBuilder): Unit = {
    val keyDecoder =
      keyDecoderOpt.map(LazyDecoder(_)).getOrElse(ConstantRefDecoder(Seq("undefined")))
    b.addEmptyLine()
    b.addBlock(s"exports.$name = damlTypes.assembleTemplate(", ");") {
      b.addBlock("{", "},") {
        b.addLine(s"templateId: '$templateId',")
        b.addInline("keyDecoder: ", ",")(keyDecoder.render(moduleId, b))
        b.addInline("keyEncode: ", ",")(keyEncode.render(moduleId, b))
        b.addInline("decoder: ", ",")(LazyDecoder(decoder).render(moduleId, b))
        b.addInline("encode: ", ",")(encode.render(moduleId, b))
        choices.foreach(_.renderJsField(moduleId, name, b))
      }
      implements.foreach(tpe => b.addLine(s"${TypeGen.renderSerializable(moduleId, tpe)},"))
    }
  }

  override def renderTsExport(b: CodeBuilder): Unit = {
    val keyType = keyDecoderOpt.map(_ => s"$name.Key").getOrElse("undefined")
    val implementsUnion =
      if (implements.nonEmpty) implements.map(TypeGen.renderType(moduleId, _)).mkString(" | ")
      else "never"
    b.addEmptyLine()
    b.addBlock(s"export declare interface ${name}Interface {", "}") {
      choices.foreach(_.renderTsTemplateField(moduleId, name, keyType, b))
    }
    b.addBlock(s"export declare const $name:", "") {
      b.addLine(s"damlTypes.Template<$name, $keyType, '$templateId'> &")
      b.addLine(s"damlTypes.ToInterface<$name, $implementsUnion> &")
      b.addLine(s"${name}Interface")
    }
  }
}

private[codegen] final case class TemplateNamespaceGen(
    moduleId: ModuleId,
    name: Name,
    key: Ast.Type,
) extends DefGen {
  override def renderJsSource(b: CodeBuilder): Unit = ()
  override def renderTsExport(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    b.addBlock(s"export declare namespace $name {", "}") {
      b.addLine(s"export type Key = ${TypeGen.renderType(moduleId, key)}")
    }
  }
}

private[codegen] final case class TemplateRegistrationGen(
    packageId: PackageId,
    packageName: PackageName,
    name: Name,
) extends DefGen {
  override def renderJsSource(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    b.addLine(s"damlTypes.registerTemplate(exports.$name, ['$packageId', '#$packageName']);")
  }

  override def renderTsExport(b: CodeBuilder): Unit = ()
}

private[codegen] final case class TypeConGen(
    moduleId: ModuleId,
    name: Name,
    paramNames: Seq[Ast.TypeVarName],
    cons: Ast.DataCons,
) extends DefGen {
  override def renderJsSource(b: CodeBuilder): Unit = ()

  override def renderTsExport(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    b.addInline("export declare ", "")(renderTsDecl(b))
  }

  def renderTsDecl(b: CodeBuilder): Unit = {
    val typeDecl =
      if (paramNames.sizeIs == 0) s"type $name"
      else s"type $name<${paramNames.mkString(", ")}>"
    cons match {
      case Ast.DataRecord(fields) =>
        b.addBlock(s"$typeDecl = {", "}") {
          fields.foreach { case (name, tpe) => b.addLine(s"$name: ${renderType(tpe)},") }
        }
      case Ast.DataVariant(variants) =>
        b.addBlock(s"$typeDecl =", "") {
          variants.foreach { case (name, tpe) =>
            b.addLine(s"| { tag: '$name'; value: ${renderType(tpe)} }")
          }
        }
      case Ast.DataEnum(constructors) =>
        b.addBlock(s"$typeDecl =", "")(constructors.foreach(name => b.addLine(s"| '$name'")))
      case Ast.DataInterface => throw new RuntimeException("interfaces are not serializable")
    }
  }

  private def renderType(tpe: Ast.Type): String = TypeGen.renderType(moduleId, tpe)
}

private[codegen] final case class SerializableGen(
    moduleId: ModuleId,
    name: ChoiceName,
    paramNames: Seq[Ast.TypeVarName],
    keys: Seq[Ast.EnumConName],
    decoder: Decoder,
    encode: Encode,
    nestedSerializables: Seq[NestedSerializable],
) extends DefGen {
  override def renderJsSource(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    if (paramNames.isEmpty) {
      b.addBlock(s"exports.$name = {", "};") {
        keys.foreach(k => b.addLine(s"$k: '$k',"))
        b.addLine(s"keys: [${keys.map(k => s"'$k'").mkString(", ")}],")
        b.addInline("decoder: ", ",")(LazyDecoder(decoder).render(moduleId, b))
        b.addInline("encode: ", ",")(encode.render(moduleId, b))
        nestedSerializables.foreach(_.renderJsField(moduleId, b))
      }
    } else {
      val params = paramNames.mkString(", ")
      b.addBlock(s"exports.$name = function ($params) {", "};") {
        b.addBlock("return ({", "});") {
          b.addInline("decoder: ", ",")(LazyDecoder(decoder).render(moduleId, b))
          b.addInline("encode: ", ",")(encode.render(moduleId, b))
        }
      }
      nestedSerializables.foreach(_.renderJsFunction(moduleId, params, b))
    }
  }

  override def renderTsExport(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    if (paramNames.isEmpty) {
      b.addBlock(s"export declare const $name:", "") {
        b.addInline(s"damlTypes.Serializable<$name>", "") {
          if (nestedSerializables.nonEmpty) {
            b.addBlock(" & {", "}")(nestedSerializables.foreach(_.renderTsField(b)))
          }
          if (keys.nonEmpty) {
            b.addLine(s" & { readonly keys: $name[] } & { readonly [e in $name]: e }")
          }
        }
      }
    } else {
      val args = paramNames.map(name => s"$name: damlTypes.Serializable<$name>").mkString(", ")
      val params = paramNames.mkString(", ")
      b.addBlock(s"export declare const $name:", "") {
        b.addInline(s"<$params>($args) => ", "") {
          b.addLine(s"damlTypes.Serializable<$name<$params>>")
          if (nestedSerializables.nonEmpty) {
            b.addBlock("& {", "}") {
              nestedSerializables.foreach(_.renderTsFunction(args, params, b))
            }
          }
        }
      }
    }
  }
}

private[codegen] final case class NestedSerializable(
    dottedName: DottedName,
    decoder: Decoder,
    encode: Encode,
) {
  val name = dottedName.segments.last

  def renderJsField(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addBlock(s"$name: {", "},") {
      b.addInline("decoder: ", ",")(LazyDecoder(decoder).render(moduleId, b))
      b.addInline("encode: ", ",")(encode.render(moduleId, b))
    }

  def renderJsFunction(moduleId: ModuleId, params: String, b: CodeBuilder): Unit =
    b.addBlock(s"exports.$dottedName = function ($params) {", "};") {
      b.addBlock("return ({", "})") {
        b.addInline("decoder: ", ",")(LazyDecoder(decoder).render(moduleId, b))
        b.addInline("encode: ", ",")(encode.render(moduleId, b))
      }
    }

  def renderTsField(b: CodeBuilder): Unit =
    b.addLine(s"$name: damlTypes.Serializable<$dottedName>;")

  def renderTsFunction(args: String, params: String, b: CodeBuilder): Unit =
    b.addLine(s"$name: (<$params>($args) => damlTypes.Serializable<$dottedName<$params>>);")
}

private[codegen] final case class InterfaceGen(
    moduleId: ModuleId,
    packageName: PackageName,
    name: Name,
    choices: Seq[ChoiceGen],
    view: TypeConId,
) extends DefGen {
  private val interfaceId = s"#$packageName:${moduleId.moduleName}:$name"
  override def renderJsSource(b: CodeBuilder): Unit = {
    b.addEmptyLine()
    b.addBlock(s"exports.$name = damlTypes.assembleInterface(", ")") {
      b.addLine(s"'$interfaceId',")
      b.addLine(s"function () { return ${TypeGen.renderSerializable(moduleId, view)}; },")
      b.addBlock("{", "}") {
        choices.foreach(_.renderJsField(moduleId, name, b))
      }
    }
  }

  override def renderTsExport(b: CodeBuilder): Unit = {
    val renderType = TypeGen.renderType(moduleId, view)
    b.addEmptyLine()
    b.addLine(s"export declare type $name = damlTypes.Interface<'$interfaceId'> & $renderType")
    b.addBlock(s"export declare interface ${name}Interface {", "}") {
      choices.foreach(_.renderTsInterfaceField(moduleId, name, b))
    }
    b.addBlock(s"export declare const $name:", "") {
      b.addLine(s"damlTypes.InterfaceCompanion<$name, undefined, '$interfaceId'> &")
      b.addLine(s"damlTypes.FromTemplate<$name, unknown> &")
      b.addLine(s"${name}Interface")
    }
  }
}

private[codegen] final case class ChoiceGen(name: Name, argType: Ast.Type, returnType: Ast.Type) {
  def renderJsField(moduleId: ModuleId, templateOrInterfaceName: Name, b: CodeBuilder): Unit =
    b.addBlock(s"$name: {", "},") {
      b.addLine(s"template: function () { return exports.$templateOrInterfaceName; },")
      b.addLine(s"choiceName: '$name',")
      b.addInline("argumentDecoder: ", ",")(LazyDecoder(TypeDecoder(argType)).render(moduleId, b))
      b.addInline("argumentEncoder: ", ",")(TypeEncode(argType).render(moduleId, b))
      b.addInline("resultDecoder: ", ",")(LazyDecoder(TypeDecoder(returnType)).render(moduleId, b))
      b.addInline("resultEncode: ", ",")(TypeEncode(returnType).render(moduleId, b))
    }

  def renderTsTemplateField(
      moduleId: ModuleId,
      templateName: Name,
      keyType: String,
      b: CodeBuilder,
  ): Unit = {
    val renderArgType: String = TypeGen.renderType(moduleId, argType)
    val renderReturnType: String = TypeGen.renderType(moduleId, returnType)
    b.addBlock(s"$name: ", "") {
      b.addLine(s"damlTypes.Choice<$templateName, $renderArgType, $renderReturnType, $keyType> &")
      b.addLine(s"damlTypes.ChoiceFrom<damlTypes.Template<$templateName, $keyType>>;")
    }
  }

  def renderTsInterfaceField(moduleId: ModuleId, interfaceName: Name, b: CodeBuilder): Unit = {
    val renderArgType: String = TypeGen.renderType(moduleId, argType)
    val renderReturnType: String = TypeGen.renderType(moduleId, returnType)
    b.addBlock(s"$name:", "") {
      b.addLine(s"damlTypes.Choice<$interfaceName, $renderArgType, $renderReturnType, undefined> &")
      b.addLine(s"damlTypes.ChoiceFrom<damlTypes.InterfaceCompanion<$interfaceName, undefined>>;")
    }
  }
}
