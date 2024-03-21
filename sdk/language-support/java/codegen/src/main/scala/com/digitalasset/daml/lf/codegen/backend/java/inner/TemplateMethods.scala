// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.ContractFilter
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.squareup.javapoet._

import javax.lang.model.element.Modifier

private[inner] object TemplateMethods {

  def apply(fields: Fields, className: ClassName)(implicit
      packagePrefixes: PackagePrefixes
  ): (Vector[MethodSpec], Seq[(ClassName, String)]) = {
    val constructor = ConstructorGenerator.generateConstructor(fields)
    val conversionMethods = distinctTypeVars(fields, IndexedSeq.empty[String]).flatMap { params =>
      val deprecatedFromValue = FromValueGenerator.generateDeprecatedFromValueForRecordLike(
        className,
        params,
      )
      val valueDecoder = FromValueGenerator.generateContractCompanionValueDecoder(
        className,
        params,
      )
      val toValue = ToValueGenerator.generateToValueForRecordLike(
        params,
        fields,
        ClassName.get(classOf[javaapi.data.DamlRecord]),
        name => CodeBlock.of("return new $T($L)", classOf[javaapi.data.DamlRecord], name),
      )
      val privateGetValueDecoder = FromValueGenerator.generateValueDecoderForRecordLike(
        fields,
        className,
        params,
        "templateValueDecoder",
        (inVar, outVar) =>
          CodeBlock.builder
            .addStatement(
              "$T $L = $L",
              classOf[javaapi.data.Value],
              outVar,
              inVar,
            )
            .build(),
        isPublic = false,
      )
      List(deprecatedFromValue, valueDecoder, toValue, privateGetValueDecoder)
    }
    val contractFilterMethod = MethodSpec
      .methodBuilder("contractFilter")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(
        ParameterizedTypeName.get(
          ClassName.get(classOf[ContractFilter[_]]),
          nestedClassName(className, "Contract"),
        )
      )
      .addStatement("return $T.of(COMPANION)", classOf[ContractFilter[_]])
      .build()

    val (jsonEncoderMethods, staticImports) = ToJsonGenerator.forRecordLike(fields, IndexedSeq())

    val jsonConversionMethods = FromJsonGenerator.forRecordLike(
      fields,
      className,
      IndexedSeq(),
    ) ++ jsonEncoderMethods

    val methods = Vector(constructor) ++ conversionMethods ++ jsonConversionMethods ++ Vector(
      contractFilterMethod
    ) ++
      ObjectMethods(className, IndexedSeq.empty[String], fields.map(_.javaName))

    (methods, staticImports)
  }
}
