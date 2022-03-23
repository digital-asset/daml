package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.iface.{
  DefDataType,
  InterfaceType,
  PrimType,
  Record,
  TemplateChoice,
  Type,
  TypeCon,
  TypeNumeric,
  TypePrim,
  TypeVar,
}
import com.squareup.javapoet._

import javax.lang.model.element.Modifier

object IdClass {

  def builder(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      packagePrefixes: Map[PackageId, String],
  ) = Builder.create(
    templateClassName,
    choices,
    packagePrefixes,
  )

  case class Builder private (
      idClassBuilder: TypeSpec.Builder,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      packagePrefixes: Map[PackageId, String],
  ) {
    def build() = idClassBuilder.build()

    def addConversionForImplementedInterfaces(implementedInterfaces: Seq[Ref.TypeConName]) = {
      implementedInterfaces.foreach { interfaceName =>
        val name = InterfaceClass.classNameForInterface(interfaceName.qualifiedName)
        val simpleName = interfaceName.qualifiedName.name.segments.last
        idClassBuilder.addMethod(
          MethodSpec
            .methodBuilder(s"to$simpleName")
            .addModifiers(Modifier.PUBLIC)
            .addStatement(s"return new $name.ContractId(this.contractId)")
            .returns(ClassName.bestGuess(s"$name.ContractId"))
            .build()
        )
      }
      this
    }

    def addFlattenedExerciseMethods(
        typeDeclarations: Map[QualifiedName, InterfaceType],
        packageId: PackageId,
    ) = {
      for ((choiceName, choice) <- choices) {
        for (
          record <- choice.param.fold(
            Builder.getRecord(_, typeDeclarations, packageId),
            _ => None,
            _ => None,
            _ => None,
          )
        ) {
          val splatted = Builder.generateFlattenedExerciseMethod(
            choiceName,
            choice,
            getFieldsWithTypes(record.fields, packagePrefixes),
            packagePrefixes,
          )
          idClassBuilder.addMethod(splatted)
        }
      }
      this
    }
  }

  private object Builder {
    private def getRecord(
        typeCon: TypeCon,
        identifierToType: Map[QualifiedName, InterfaceType],
        packageId: PackageId,
    ): Option[Record.FWT] = {
      // TODO: at the moment we don't support other packages Records because the codegen works on single packages
      if (typeCon.name.identifier.packageId == packageId) {
        identifierToType.get(typeCon.name.identifier.qualifiedName) match {
          case Some(InterfaceType.Normal(DefDataType(_, record: Record.FWT))) =>
            Some(record)
          case _ => None
        }
      } else None
    }

    private def generateFlattenedExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
        fields: Fields,
        packagePrefixes: Map[PackageId, String],
    ): MethodSpec = {
      val methodName = s"exercise${choiceName.capitalize}"
      val exerciseChoiceBuilder = MethodSpec
        .methodBuilder(methodName)
        .addModifiers(Modifier.PUBLIC)
        .returns(classOf[javaapi.data.ExerciseCommand])
      val javaType = toJavaTypeName(choice.param, packagePrefixes)
      for (FieldInfo(_, _, javaName, javaType) <- fields) {
        exerciseChoiceBuilder.addParameter(javaType, javaName)
      }
      exerciseChoiceBuilder.addStatement(
        "return $L(new $T($L))",
        methodName,
        javaType,
        generateArgumentList(fields.map(_.javaName)),
      )
      exerciseChoiceBuilder.build()
    }

    private[inner] def generateExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
        templateClassName: ClassName,
        packagePrefixes: Map[PackageId, String],
    ): MethodSpec = {
      val methodName = s"exercise${choiceName.capitalize}"
      val exerciseChoiceBuilder = MethodSpec
        .methodBuilder(methodName)
        .addModifiers(Modifier.PUBLIC)
        .returns(classOf[javaapi.data.ExerciseCommand])
      val javaType = toJavaTypeName(choice.param, packagePrefixes)
      exerciseChoiceBuilder.addParameter(javaType, "arg")
      choice.param match {
        case TypeCon(_, _) =>
          exerciseChoiceBuilder.addStatement(
            "$T argValue = arg.toValue()",
            classOf[javaapi.data.Value],
          )
        case TypePrim(PrimType.Unit, ImmArraySeq()) =>
          exerciseChoiceBuilder
            .addStatement(
              "$T argValue = $T.getInstance()",
              classOf[javaapi.data.Value],
              classOf[javaapi.data.Unit],
            )
        case TypePrim(_, _) | TypeVar(_) | TypeNumeric(_) =>
          exerciseChoiceBuilder
            .addStatement(
              "$T argValue = new $T(arg)",
              classOf[javaapi.data.Value],
              toAPITypeName(choice.param),
            )
      }
      exerciseChoiceBuilder.addStatement(
        "return new $T($T.TEMPLATE_ID, this.contractId, $S, argValue)",
        classOf[javaapi.data.ExerciseCommand],
        templateClassName,
        choiceName,
      )
      exerciseChoiceBuilder.build()
    }

    def create(
        templateClassName: ClassName,
        choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
        packagePrefixes: Map[PackageId, String],
    ): Builder = {

      val idClassBuilder =
        TypeSpec
          .classBuilder("ContractId")
          .superclass(
            ParameterizedTypeName
              .get(ClassName.get(classOf[javaapi.data.codegen.ContractId[_]]), templateClassName)
          )
          .addModifiers(Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
      val constructor =
        MethodSpec
          .constructorBuilder()
          .addModifiers(Modifier.PUBLIC)
          .addParameter(ClassName.get(classOf[String]), "contractId")
          .addStatement("super(contractId)")
          .build()
      idClassBuilder.addMethod(constructor)
      for ((choiceName, choice) <- choices) {
        val exerciseChoiceMethod =
          generateExerciseMethod(choiceName, choice, templateClassName, packagePrefixes)
        idClassBuilder.addMethod(exerciseChoiceMethod)
      }
      Builder(idClassBuilder, choices, packagePrefixes)
    }
  }
}
