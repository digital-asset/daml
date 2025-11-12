// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{DottedName, ModuleId, Name}
import com.digitalasset.daml.lf.language.Ast

private[codegen] sealed trait Decoder {
  def render(currentModule: ModuleId, indent: String): String
}

private[codegen] final case class OneOfDecoder(decoders: Seq[Decoder]) extends Decoder {
  override def render(currentModule: ModuleId, indent: String): String = {
    val branches = decoders.map(_.render(currentModule, indent + "  "))
    s"""|jtv.oneOf(
        |$indent  ${branches.mkString(s",\n$indent  ")}
        |$indent)""".stripMargin
  }
}

private[codegen] final case class ObjectDecoder(fields: Seq[(String, Decoder)]) extends Decoder {
  override def render(currentModule: ModuleId, indent: String): String = {
    val decodedFields = fields
      .map { case (name, field) => s"$name: ${field.render(currentModule, indent + "  ")}" }
    s"""|jtv.object({
        |$indent  ${decodedFields.mkString(s",\n$indent  ")}
        |$indent})""".stripMargin
  }
}

private[codegen] final case class ConstantStringDecoder(value: String) extends Decoder {
  override def render(currentModule: ModuleId, indent: String): String =
    s"""jtv.constant("$value")"""
}

private[codegen] final case class ConstantRefDecoder(ref: Seq[String]) extends Decoder {
  override def render(currentModule: ModuleId, indent: String): String =
    s"""jtv.constant(${ref.mkString(".")})"""
}

private[codegen] final case class TypeDecoder(tpe: Ast.Type) extends Decoder {
  override def render(currentModule: ModuleId, indent: String): String = {
    val decoder = TypeGen.renderSerializable(currentModule, tpe) + ".decoder"
    tpe match {
      case Ast.TApp(Ast.TBuiltin(Ast.BTOptional), _) => s"jtv.Decoder.withDefault(null, $decoder)"
      case _ => decoder
    }
  }
}

private[codegen] final case class LazyDecoder(underlying: Decoder) extends Decoder {
  override def render(currentModule: ModuleId, indent: String): String =
    s"""|damlTypes.lazyMemo(function () {
        |$indent  return ${underlying.render(currentModule, indent + "  ")};
        |$indent})""".stripMargin
}

private[codegen] object Decoder {
  def apply(dataConsName: Name, dataCons: Ast.DataCons): Decoder =
    apply(DottedName.assertFromNames(ImmArray(dataConsName)), dataCons)

  def apply(dataConsName: DottedName, dataCons: Ast.DataCons): Decoder =
    dataCons match {
      case Ast.DataRecord(fields) =>
        ObjectDecoder(fields.toSeq.map { case (name, tpe) => name -> TypeDecoder(tpe) })
      case Ast.DataVariant(variants) =>
        OneOfDecoder(
          variants.toSeq.map { case (name, tpe) =>
            ObjectDecoder(Seq("tag" -> ConstantStringDecoder(name), "value" -> TypeDecoder(tpe)))
          }
        )
      case Ast.DataEnum(constructors) =>
        OneOfDecoder(
          constructors.toSeq.map { cons =>
            ConstantRefDecoder(Seq("exports") ++ dataConsName.segments.toSeq ++ Seq(cons))
          }
        )
      case Ast.DataInterface => throw new RuntimeException("interfaces are not serializable")
    }
}
