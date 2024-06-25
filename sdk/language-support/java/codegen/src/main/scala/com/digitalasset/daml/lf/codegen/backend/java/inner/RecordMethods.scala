// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.digitalasset.daml.lf.codegen.backend.java.ObjectMethods
import com.squareup.javapoet._

private[inner] object RecordMethods {

  def apply(fields: Fields, className: ClassName, typeParameters: IndexedSeq[String])(implicit
      packagePrefixes: PackagePrefixes
  ): (Vector[MethodSpec], Seq[(ClassName, String)]) = {

    val constructor = ConstructorGenerator.generateConstructor(fields)

    val conversionMethods = distinctTypeVars(fields, typeParameters).flatMap { params =>
      val valueDecoder = FromValueGenerator.generateValueDecoderForRecordLike(
        fields,
        className.parameterized(typeParameters),
        params,
        "valueDecoder",
        (inVar, outVar) =>
          CodeBlock.builder
            .addStatement(
              "$T $L = $L",
              classOf[javaapi.data.Value],
              outVar,
              inVar,
            )
            .build(),
      )
      val toValue = ToValueGenerator.generateToValueForRecordLike(
        params,
        fields,
        ClassName.get(classOf[javaapi.data.DamlRecord]),
        name => CodeBlock.of("return new $T($L)", classOf[javaapi.data.DamlRecord], name),
      )
      List(valueDecoder, toValue)
    }

    val (jsonEncoders, staticImports) = ToJsonGenerator.forRecordLike(fields, typeParameters)

    val jsonConversionMethods = FromJsonGenerator.forRecordLike(
      fields,
      className,
      typeParameters,
    ) ++ jsonEncoders

    val methods = Vector(constructor) ++ conversionMethods ++ jsonConversionMethods ++
      ObjectMethods(className, typeParameters, fields.map(_.javaName))

    (methods, staticImports)
  }
}
