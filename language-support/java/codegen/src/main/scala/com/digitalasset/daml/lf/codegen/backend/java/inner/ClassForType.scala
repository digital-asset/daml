// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner
import com.digitalasset.daml.lf.codegen.TypeWithContext
import com.digitalasset.daml.lf.codegen.backend.java.JavaEscaper
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.iface.InterfaceType.{Normal, Template}
import com.digitalasset.daml.lf.iface.{Enum, DefDataType, Record, Variant}
import com.squareup.javapoet.{ClassName, FieldSpec, JavaFile, TypeSpec}
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier

object ClassForType extends StrictLogging {

  def apply(
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String]): List[JavaFile] = {

    val className =
      ClassName.bestGuess(fullyQualifiedName(typeWithContext.identifier, packagePrefixes))
    val javaPackage = className.packageName()

    typeWithContext.`type`.typ match {

      case Some(Normal(DefDataType(typeVars, record: Record.FWT))) =>
        val typeSpec =
          RecordClass.generate(
            className,
            typeVars.map(JavaEscaper.escapeString),
            record,
            None,
            packagePrefixes)
        List(javaFile(typeWithContext, javaPackage, typeSpec))

      case Some(Normal(DefDataType(typeVars, variant: Variant.FWT))) =>
        val subPackage = className.packageName() + "." + JavaEscaper.escapeString(
          className.simpleName().toLowerCase)
        val (tpe, constructors) =
          VariantClass.generate(
            className,
            subPackage,
            typeVars.map(JavaEscaper.escapeString),
            variant,
            typeWithContext,
            packagePrefixes)
        javaFile(typeWithContext, javaPackage, tpe) ::
          constructors.map(cons => javaFile(typeWithContext, subPackage, cons))

      case Some(Normal(DefDataType(_, enum: Enum))) =>
        val subPackage = className.packageName() + "." + JavaEscaper.escapeString(
          className.simpleName().toLowerCase)
        List(
          JavaFile
            .builder(javaPackage, EnumClass.generate(className, typeWithContext.identifier, enum))
            .build())

      case Some(Template(record, template)) =>
        val typeSpec =
          TemplateClass.generate(className, record, template, typeWithContext, packagePrefixes)
        List(JavaFile.builder(javaPackage, typeSpec).build())

      case None =>
        // This typeWithContext didn't contain a type itself, but has children nodes
        // which we treat as any other TypeWithContext
        typeWithContext.typesLineages.flatMap(ClassForType(_, packagePrefixes)).toList
    }
  }

  def javaFile(typeWithContext: TypeWithContext, javaPackage: String, typeSpec: TypeSpec) = {
    val withField =
      typeSpec.toBuilder.addField(createPackageIdField(typeWithContext.interface.packageId)).build()
    JavaFile.builder(javaPackage, withField).build()
  }

  private def createPackageIdField(packageId: PackageId): FieldSpec = {
    FieldSpec
      .builder(classOf[String], "_packageId", Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
      .initializer("$S", packageId)
      .build()
  }
}
