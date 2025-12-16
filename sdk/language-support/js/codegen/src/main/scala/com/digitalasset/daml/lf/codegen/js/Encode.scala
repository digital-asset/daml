// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{DottedName, ModuleId, Name}
import com.digitalasset.daml.lf.language.Ast

private[codegen] sealed trait Encode {
  def render(moduleId: ModuleId, b: CodeBuilder): Unit
}

private[codegen] final case class VariantEncode(
    conName: DottedName,
    variants: Seq[(Name, Ast.Type)],
) extends Encode {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit = {
    def renderSer(tpe: Ast.Type): String = TypeGen.renderSerializable(moduleId, tpe)
    b.addBlock("function (__typed__) {", "}") {
      b.addBlock("switch(__typed__.tag) {", "}") {
        variants.foreach { case (name, tpe) =>
          b.addLine(
            s"case '$name': return {tag: __typed__.tag, value: ${renderSer(tpe)}.encode(__typed__.value)};"
          )
        }
        b.addLine(
          s"default: throw 'unrecognized type tag: ' + __typed__.tag + ' while serializing a value of type $conName';"
        )
      }
    }
  }
}

private[codegen] final case class RecordEncode(fields: Seq[(Name, Ast.Type)]) extends Encode {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addBlock("function (__typed__) {", "}") {
      if (fields.nonEmpty) {
        b.addBlock("return {", "};") {
          fields.foreach { case (name, tpe) =>
            b.addLine(
              s"$name: ${TypeGen.renderSerializable(moduleId, tpe)}.encode(__typed__.$name),"
            )
          }
        }
      } else b.addLine("return {};")
    }
}

private[codegen] object IdentityEncode extends Encode {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addLine("function (__typed__) { return __typed__; }")
}

private[codegen] object ThrowEncode extends Encode {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit =
    b.addLine("function () { throw 'EncodeError'; }")
}

private[codegen] final case class TypeEncode(tpe: Ast.Type) extends Encode {
  override def render(moduleId: ModuleId, b: CodeBuilder): Unit = {
    val ser = TypeGen.renderSerializable(moduleId, tpe)
    b.addLine(s"function (__typed__) { return $ser.encode(__typed__); }")
  }
}

private[codegen] object Encode {
  def apply(dataConName: Name, dataCon: Ast.DataCons): Encode =
    apply(DottedName.unsafeFromNames(ImmArray(dataConName)), dataCon)

  def apply(dataConName: DottedName, dataCon: Ast.DataCons): Encode =
    dataCon match {
      case Ast.DataRecord(fields) => RecordEncode(fields.toSeq)
      case Ast.DataVariant(variants) => VariantEncode(dataConName, variants.toSeq)
      case Ast.DataEnum(constructors) => IdentityEncode
      case Ast.DataInterface => throw new RuntimeException("interfaces are not serializable")
    }
}
