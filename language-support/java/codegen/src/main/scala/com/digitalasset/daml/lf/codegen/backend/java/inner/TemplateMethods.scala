// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.daml.lf.data.Ref.PackageId
import com.squareup.javapoet._

private[inner] object TemplateMethods {

  def apply(
      fields: Fields,
      className: ClassName,
      packagePrefixes: Map[PackageId, String],
  ): Vector[MethodSpec] = {
    val constructor = ConstructorGenerator.generateConstructor(fields)
    val conversionMethods = distinctTypeVars(fields, IndexedSeq.empty[String]).flatMap { params =>
      val deprecatedFromValue = FromValueGenerator.generateDeprecatedFromValueForRecordLike(
        className,
        params,
      )
      val fromValue = FromValueGenerator.generateContractCompanionValueDecoder(
        className,
        params,
      )
      val toValue = ToValueGenerator.generateToValueForRecordLike(
        params,
        fields,
        packagePrefixes,
        ClassName.get(classOf[javaapi.data.DamlRecord]),
        name => CodeBlock.of("return new $T($L)", classOf[javaapi.data.DamlRecord], name),
      )
      val privateGetValueDecoder = FromValueGenerator.generateFromValueForRecordLike(
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
        packagePrefixes,
        isPublic = false,
      )
      List(deprecatedFromValue, fromValue, toValue, privateGetValueDecoder)
    }

    Vector(constructor) ++ conversionMethods ++
      ObjectMethods(className, IndexedSeq.empty[String], fields.map(_.javaName))
  }
}
