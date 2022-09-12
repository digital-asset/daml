// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.data.Ref.{PackageId, Identifier}
import com.daml.lf.typesig.{Enum, DefDataType, PackageSignature, Record, Variant}
import PackageSignature.TypeDecl
import TypeDecl.{Normal, Template}
import com.squareup.javapoet.{ClassName, JavaFile, TypeSpec}
import com.typesafe.scalalogging.StrictLogging

object ClassForType extends StrictLogging {

  def apply(
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String],
      toBeGenerated: Identifier => Boolean,
  ): List[JavaFile] = {

    def recurOnTypeLineages: List[JavaFile] =
      typeWithContext.typesLineages
        .flatMap(ClassForType(_, packagePrefixes, toBeGenerated))
        .toList

    def generateForType(lfInterfaceType: TypeDecl): List[JavaFile] = {
      val classNameString = fullyQualifiedName(typeWithContext.identifier, packagePrefixes)
      val className = ClassName.bestGuess(classNameString)
      generateInterfaceTypes(typeWithContext, packagePrefixes) ++
        generateSerializableTypes(typeWithContext, className, packagePrefixes, lfInterfaceType)
    }

    Option
      .when(toBeGenerated(typeWithContext.identifier))(typeWithContext.`type`.typ)
      .flatten
      .fold(recurOnTypeLineages)(generateForType)

  }

  private def generateInterfaceTypes(
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String],
  ): List[JavaFile] =
    for {
      (interfaceName, interface) <- typeWithContext.interface.interfaces.toList
      className = ClassName.bestGuess(fullyQualifiedName(interfaceName))
      packageName = className.packageName()
      interfaceClass =
        InterfaceClass
          .generate(
            className,
            interface,
            packagePrefixes,
            typeWithContext.interface.typeDecls,
            typeWithContext.interface.packageId,
            interfaceName,
          )
    } yield javaFile(packageName, interfaceClass)

  private def generateSerializableTypes(
      typeWithContext: TypeWithContext,
      className: ClassName,
      packagePrefixes: Map[PackageId, String],
      lfInterfaceType: TypeDecl,
  ): List[JavaFile] = {
    val packageName = className.packageName()
    lfInterfaceType match {
      case Normal(DefDataType(typeVars, record: Record.FWT)) =>
        val recordClass =
          RecordClass.generate(
            typeWithContext.interface.packageId,
            className,
            typeVars.map(JavaEscaper.escapeString),
            record,
            packagePrefixes,
          )
        javaFiles(packageName, recordClass)
      case Normal(DefDataType(typeVars, variant: Variant.FWT)) =>
        val simpleLowerCaseName = JavaEscaper.escapeString(className.simpleName().toLowerCase)
        val subPackage = s"$packageName.$simpleLowerCaseName"
        val escapedTypeVars = typeVars.map(JavaEscaper.escapeString)
        val (variantSpec, constructorSpecs) =
          VariantClass.generate(
            className,
            subPackage,
            escapedTypeVars,
            variant,
            typeWithContext,
            packagePrefixes,
          )
        javaFile(packageName, variantSpec) :: javaFiles(subPackage, constructorSpecs)
      case Normal(DefDataType(_, enum: Enum)) =>
        javaFiles(packageName, EnumClass.generate(className, enum))
      case Template(record, template) =>
        val typeSpec =
          TemplateClass.generate(
            className,
            record,
            template,
            typeWithContext,
            packagePrefixes,
          )
        javaFiles(packageName, typeSpec)
    }
  }

  private def javaFile(packageName: String, typeSpec: TypeSpec): JavaFile =
    JavaFile.builder(packageName, typeSpec).build()

  private def javaFiles(packageName: String, typeSpec: TypeSpec): List[JavaFile] =
    List(javaFile(packageName, typeSpec))

  private def javaFiles(packageName: String, typeSpecs: List[TypeSpec]): List[JavaFile] =
    typeSpecs.map(javaFile(packageName, _))

}
