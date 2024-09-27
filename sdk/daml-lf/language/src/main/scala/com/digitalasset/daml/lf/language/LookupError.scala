// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data.Ref._

sealed abstract class LookupError {
  def pretty: String
}

object LookupError {

  private[lf] def contextDetails(context: Reference): String =
    context match {
      case Reference.Package(_) => ""
      case otherwise => " while looking for " + otherwise.pretty
    }

  final case class NotFound(notFound: Reference, context: Reference) extends LookupError {
    override def pretty: String = "unknown " + notFound.pretty + (
      if (context == notFound) "" else LookupError.contextDetails(context)
    )
  }

  object MissingPackage {
    def unapply(err: NotFound): Option[(PackageRef, Reference)] =
      err.notFound match {
        case Reference.Package(packageRef) => Some(packageRef -> err.context)
        case _ => None
      }

    def apply(pkgRef: PackageRef): NotFound = {
      val ref = Reference.Package(pkgRef)
      LookupError.NotFound(ref, ref)
    }

    def apply(pkgId: PackageId): NotFound = apply(PackageRef.Id(pkgId))

    def pretty(pkgRef: PackageRef, context: Reference): String =
      s"Couldn't find package $pkgRef" + contextDetails(context)

    def pretty(pkgId: PackageId, context: Reference): String = pretty(PackageRef.Id(pkgId), context)
  }

}

sealed abstract class Reference extends Product with Serializable {
  def pretty: String
}

object Reference {

  final case class Package(packageRef: PackageRef) extends Reference {
    override def pretty: String = s"package $packageRef"
  }
  object Package { def apply(packageId: PackageId): Package = apply(PackageRef.Id(packageId)) }

  final case class Module(packageId: PackageId, moduleName: ModuleName) extends Reference {
    override def pretty: String = s"module $packageId:$moduleName"
  }

  final case class Definition(identifier: Identifier) extends Reference {
    override def pretty: String = s"definition $identifier"
  }

  final case class TypeSyn(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"type synonym $tyCon"
  }

  final case class DataType(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"data type $tyCon"
  }

  final case class DataRecord(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"record $tyCon"
  }

  final case class DataRecordField(tyCon: TypeConName, fieldName: Ast.FieldName) extends Reference {
    override def pretty: String = s"record field $fieldName in record $tyCon"
  }

  final case class DataVariant(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"variant $tyCon"
  }

  final case class DataVariantConstructor(
      tyCon: TypeConName,
      constructorName: Ast.VariantConName,
  ) extends Reference {
    override def pretty: String = s"constructor $constructorName in variant $tyCon"
  }

  final case class DataEnum(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"enumeration $tyCon"
  }

  final case class DataEnumConstructor(tyCon: TypeConName, constructorName: Ast.EnumConName)
      extends Reference {
    override def pretty: String = s"constructor $constructorName in enumeration $tyCon"
  }

  final case class Value(identifier: Identifier) extends Reference {
    override def pretty: String = s"value $identifier"
  }

  final case class Template(tyCon: TypeConRef) extends Reference {
    override def pretty: String = s"template $tyCon"
  }

  object Template {
    def apply(tyCon: TypeConName): Template =
      Template(tyCon.toRef)
  }

  final case class Interface(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"interface $tyCon"
  }

  final case class TemplateKey(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"template without contract key $tyCon."
  }

  /** References an non-retroactive interface implementation of interfaceName for templateName.
    */
  final case class InterfaceInstance(interfaceName: TypeConName, templateName: TypeConName)
      extends Reference {
    override def pretty: String = s"interface instance $interfaceName for $templateName"
  }

  final case class TemplateChoice(tyCon: TypeConName, choiceName: ChoiceName) extends Reference {
    override def pretty: String = s"choice $choiceName in template $tyCon"
  }

  final case class InterfaceChoice(tyCon: TypeConName, choiceName: ChoiceName) extends Reference {
    override def pretty: String = s"choice $choiceName in interface $tyCon"
  }

  final case class InheritedChoice(
      ifaceName: TypeConName,
      templateName: TypeConName,
      choiceName: ChoiceName,
  ) extends Reference {
    override def pretty: String =
      s"choice $choiceName in template $templateName by interface $ifaceName"
  }

  final case class TemplateOrInterface(tyCon: TypeConRef) extends Reference {
    override def pretty: String = s"template or interface $tyCon"
  }

  object TemplateOrInterface {
    def apply(tyCon: TypeConName): TemplateOrInterface =
      TemplateOrInterface(tyCon.toRef)
  }

  final case class Choice(tyCon: TypeConName, choiceName: ChoiceName) extends Reference {
    override def pretty: String = s"choice $choiceName in template or interface $tyCon"
  }

  final case class Method(tyCon: TypeConName, methodName: MethodName) extends Reference {
    override def pretty: String = s"method $methodName in interface $tyCon"
  }

  final case class Exception(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"exception $tyCon"
  }

}
