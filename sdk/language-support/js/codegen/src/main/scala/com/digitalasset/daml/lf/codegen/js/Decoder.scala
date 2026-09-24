// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{DottedName, ModuleId, Name}
import com.digitalasset.daml.lf.language.Ast

private[codegen] sealed trait Decoder {
  def render(moduleId: ModuleId, builder: CodeBuilder): Unit
}

private[codegen] final case class OneOfDecoder(decoders: Seq[Decoder]) extends Decoder {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addBlock("jtv.oneOf(", ")") {
      decoders.foreach(d => b.addInline("", ",")(d.render(moduleId, b)))
    }
}

private[codegen] final case class ObjectDecoder(fields: Seq[(String, Decoder)]) extends Decoder {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addBlock("jtv.object({", "})") {
      fields.foreach { case (name, field) =>
        b.addInline(s"$name: ", ",")(field.render(moduleId, b))
      }
    }
}

private[codegen] final case class ConstantStringDecoder(value: String) extends Decoder {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addLine(s"""jtv.constant("$value")""")
}

private[codegen] final case class ConstantRefDecoder(ref: Seq[String]) extends Decoder {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addLine(s"""jtv.constant(${ref.mkString(".")})""")
}

private[codegen] final case class TypeDecoder(tpe: Ast.Type) extends Decoder {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit = {
    val decoder = TypeGen.renderSerializable(moduleId, tpe) + ".decoder"
    tpe match {
      case Ast.TApp(Ast.TBuiltin(Ast.BTOptional), _) =>
        b.addLine(s"jtv.Decoder.withDefault(null, $decoder)")
      case _ => b.addLine(decoder)
    }
  }
}

private[codegen] final case class LazyDecoder(underlying: Decoder) extends Decoder {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addBlock("damlTypes.lazyMemo(function () {", "})") {
      b.addInline("return ", ";")(underlying.render(moduleId, b))
    }
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
