// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.digitalasset.daml.lf.data.Ref
import Ref.{ChoiceName, PackageId, PackageName, PackageRef, PackageVersion}
import com.digitalasset.daml.lf.typesig.{DefDataType, Record, TypeCon}
import com.digitalasset.daml.lf.typesig.PackageSignature.TypeDecl
import java.util.Optional
import com.digitalasset.daml.lf.typesig._
import com.squareup.javapoet._
import com.daml.ledger.javaapi
import com.digitalasset.daml.lf.codegen.NodeWithContext.AuxiliarySignatures

import javax.lang.model.element.Modifier

private[inner] object ClassGenUtils {

  val optionalString = ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])

  def optional(name: TypeName) =
    ParameterizedTypeName.get(ClassName.get(classOf[Optional[_]]), name)

  def setOfStrings = ParameterizedTypeName.get(classOf[java.util.Set[_]], classOf[String])

  val emptyOptional = CodeBlock.of("$T.empty()", classOf[Optional[_]])
  val emptySet = CodeBlock.of("$T.emptySet()", classOf[java.util.Collections])
  val getContractId = CodeBlock.of("event.getContractId()")

  def getContractKey(t: Type)(implicit packagePrefixes: PackagePrefixes) =
    CodeBlock.of(
      "event.getContractKey().map(e -> $L)",
      FromValueGenerator.extractor(t, "e", CodeBlock.of("e"), newNameGenerator),
    )

  val getSignatories = CodeBlock.of("event.getSignatories()")
  val getObservers = CodeBlock.of("event.getObservers()")

  def getRecord(
      typeCon: TypeCon,
      identifierToType: AuxiliarySignatures,
  ): Option[Record.FWT] = {
    val TypeCon(TypeConId(Ref.Identifier(packageId, qualName)), _) = typeCon
    identifierToType get packageId flatMap (_.typeDecls get qualName) collect {
      case TypeDecl.Normal(DefDataType(_, record: Record.FWT)) =>
        record
    }
  }

  val templateIdFieldName = "TEMPLATE_ID"
  val templateIdWithPackageIdFieldName = "TEMPLATE_ID_WITH_PACKAGE_ID"
  val interfaceIdFieldName = "INTERFACE_ID"
  val interfaceIdWithPackageIdFieldName = "INTERFACE_ID_WITH_PACKAGE_ID"
  val packageIdFieldName = "PACKAGE_ID"
  val packageNameFieldName = "PACKAGE_NAME"
  val packageVersionFieldName = "PACKAGE_VERSION"
  val companionFieldName = "COMPANION"
  val archiveChoiceName = ChoiceName assertFromString "Archive"

  def generateTemplateIdFields(
      pkgId: PackageId,
      pkgName: PackageName,
      moduleName: String,
      name: String,
  ): Seq[FieldSpec] =
    generateEntityIdFields(
      pkgId,
      pkgName,
      moduleName,
      name,
      templateIdFieldName,
      templateIdWithPackageIdFieldName,
    )

  def generateInterfaceIdFields(
      pkgId: PackageId,
      pkgName: PackageName,
      moduleName: String,
      name: String,
  ): Seq[FieldSpec] =
    generateEntityIdFields(
      pkgId,
      pkgName,
      moduleName,
      name,
      interfaceIdFieldName,
      interfaceIdWithPackageIdFieldName,
    )

  def generateEntityIdFields(
      pkgId: PackageId,
      pkgName: PackageName,
      moduleName: String,
      name: String,
      entityIdFieldName: String,
      entityIdWithPackageIdFieldName: String,
  ): Seq[FieldSpec] = {
    val packageRef = PackageRef.Name(pkgName)
    def idField(fieldName: String, pkg: String) =
      FieldSpec
        .builder(
          ClassName.get(classOf[javaapi.data.Identifier]),
          fieldName,
          Modifier.STATIC,
          Modifier.FINAL,
          Modifier.PUBLIC,
        )
        .initializer(
          "new $T($S, $S, $S)",
          classOf[javaapi.data.Identifier],
          pkg,
          moduleName,
          name,
        )
        .build()
    Seq(
      idField(entityIdFieldName, packageRef.toString),
      idField(entityIdWithPackageIdFieldName, pkgId.toString),
    )
  }

  def generatePackageIdField(packageId: PackageId) =
    FieldSpec
      .builder(
        ClassName.get(packageId.getClass),
        packageIdFieldName,
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer("$S", packageId)
      .build()

  def generatePackageNameField(packageName: PackageName) =
    FieldSpec
      .builder(
        ClassName.get(packageName.getClass),
        packageNameFieldName,
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer("$S", packageName)
      .build()

  def generatePackageVersionField(packageVersion: PackageVersion) = {
    val packageVersionSegmentIntArrLiteral =
      packageVersion.segments.toArray.mkString("{", ", ", "}")
    val intArrayTypeName = ArrayTypeName.of(classOf[Int])
    FieldSpec
      .builder(
        ClassName.get(classOf[javaapi.data.PackageVersion]),
        packageVersionFieldName,
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer(
        CodeBlock
          .builder()
          .add(
            "new $T(new $T $L)",
            ClassName.get(classOf[javaapi.data.PackageVersion]),
            intArrayTypeName,
            packageVersionSegmentIntArrLiteral,
          )
          .build()
      )
      .build()
  }

  def generateFlattenedCreateOrExerciseMethod(
      name: String,
      returns: TypeName,
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
  )(alter: MethodSpec.Builder => MethodSpec.Builder)(implicit
      packagePrefixes: PackagePrefixes
  ): MethodSpec = {
    val methodName = s"$name${choiceName.capitalize}"
    val choiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(returns)
    val javaType = toJavaTypeName(choice.param)
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      choiceBuilder.addParameter(javaType, javaName)
    }
    choiceBuilder.addStatement(
      "return $L(new $T($L))",
      methodName,
      javaType,
      generateArgumentList(fields.map(_.javaName)),
    )
    alter(choiceBuilder).build()
  }

  def generateGetCompanion(companionType: TypeName, companionName: String): MethodSpec = {
    MethodSpec
      .methodBuilder("getCompanion")
      .addModifiers(Modifier.PROTECTED)
      .addAnnotation(classOf[Override])
      .returns(companionType)
      .addStatement("return $N", companionName)
      .build()
  }
}
