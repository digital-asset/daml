// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{DottedName, ModuleId, Name}
import com.digitalasset.daml.lf.language.Ast

private[codegen] sealed trait Encode {
  def render(currentModule: ModuleId, indent: String): String
}

private[codegen] final case class VariantEncode(
    conName: DottedName,
    variants: Seq[(Name, Ast.Type)],
) extends Encode {
  override def render(currentModule: ModuleId, indent: String): String = {
    val cases = variants.map { case (name, tpe) =>
      s"case '$name': return {tag: __typed__.tag, value: ${TypeGen.renderSerializable(currentModule, tpe)}.encode(__typed__.value)};"
    }
    s"""|function (__typed__) {
        |$indent  switch(__typed__.tag) {
        |$indent    ${cases.mkString(s"\n$indent    ")}
        |$indent    default: throw 'unrecognized type tag: ' + __typed__.tag + ' while serializing a value of type $conName';
        |$indent  }
        |$indent}""".stripMargin
  }
}

private[codegen] final case class RecordEncode(fields: Seq[(Name, Ast.Type)]) extends Encode {
  override def render(currentModule: ModuleId, indent: String): String = {
    val fieldsEncode = fields.map { case (name, tpe) =>
      s"$name: ${TypeGen.renderSerializable(currentModule, tpe)}.encode(__typed__.$name),"
    }
    s"""|function (__typed__) {
        |$indent  return {
        |$indent    ${fieldsEncode.mkString(s"\n$indent    ")}
        |$indent  };
        |$indent}""".stripMargin
  }
}

private[codegen] object IdentityEncode extends Encode {
  override def render(currentModule: ModuleId, indent: String): String =
    "function (__typed__) { return __typed__; }"
}

private[codegen] object ThrowEncode extends Encode {
  override def render(currentModule: ModuleId, indent: String): String =
    "function () { throw 'EncodeError'; }"
}

private[codegen] final case class TypeEncode(tpe: Ast.Type) extends Encode {
  override def render(currentModule: ModuleId, indent: String): String =
    s"function (__typed__) { return ${TypeGen.renderSerializable(currentModule, tpe)}.encode(__typed__); }"
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
