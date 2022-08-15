// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref._
import com.daml.lf.language.{TemplateOrInterface => TorI}

sealed abstract class LookupError {
  def pretty: String
}

object LookupError {

  def contextDetails(context: Reference): String =
    context match {
      case Reference.Package(_) => ""
      case otherwise => " while looking for " + otherwise.pretty
    }

  final case class NotFound(notFound: Reference, context: Reference) extends LookupError {
    def pretty: String = "unknown " + notFound.pretty + (
      if (context == notFound) "" else LookupError.contextDetails(context)
    )
  }

  final case class AmbiguousInterfaceInstance(instance: Reference.InterfaceInstance)
      extends LookupError {
    def pretty: String =
      s"Ambiguous interface instance: two instances for ${instance.pretty}"
  }

  object MissingPackage {
    def unapply(err: LookupError.NotFound): Option[(PackageId, Reference)] =
      err.notFound match {
        case Reference.Package(packageId) => Some(packageId -> err.context)
        case _ => None
      }

    def pretty(pkgId: PackageId, context: Reference): String =
      s"Couldn't find package $pkgId" + contextDetails(context)
  }

  def apply(notFound: Reference, context: Reference) =
    NotFound(notFound, context)

  def unapply(err: LookupError.NotFound): Option[(Reference, Reference)] =
    Some(err.notFound, err.context)

}

sealed abstract class Reference extends Product with Serializable {
  def pretty: String
}

object Reference {

  final case class Package(packageId: PackageId) extends Reference {
    override def pretty: String = s"package $packageId."
  }

  final case class Module(packageId: PackageId, moduleName: ModuleName) extends Reference {
    override def pretty: String = s"module $packageId:$moduleName"
  }

  final case class Definition(identifier: Ref.Identifier) extends Reference {
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

  final case class Value(identifier: Ref.Identifier) extends Reference {
    override def pretty: String = s"value $identifier"
  }

  final case class Template(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"template $tyCon"
  }

  final case class Interface(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"interface $tyCon"
  }

  final case class TemplateKey(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"template without contract key $tyCon."
  }

  /** References an interface implementation of interfaceName for templateName,
    * if a unique one exists.
    */
  final case class InterfaceInstance(interfaceName: TypeConName, templateName: TypeConName)
      extends Reference {
    override def pretty: String = s"interface instance $interfaceName for $templateName"
  }

  /** References an interface implementation of interfaceName for templateName
    * defined in templateName if parentTemplateOrInterface == TorI.Template(())
    * or in interfaceName if parentTemplateOrInterface == TorI.Interface(())
    */
  final case class ConcreteInterfaceInstance(
      parentTemplateOrInterface: TorI[Unit, Unit],
      interfaceInstance: InterfaceInstance,
  ) extends Reference {

    def parent: TorI[TypeConName, TypeConName] = parentTemplateOrInterface match {
      case TorI.Template(()) => TorI.Template(interfaceInstance.templateName)
      case TorI.Interface(()) => TorI.Interface(interfaceInstance.interfaceName)
    }

    def prettyParent: String = parent match {
      case TorI.Template(t) => s"template $t"
      case TorI.Interface(i) => s"interface $i"
    }

    override def pretty: String =
      s"$prettyParent-provided $interfaceInstance.pretty"
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

  final case class TemplateOrInterface(tyCon: TypeConName) extends Reference {
    override def pretty: String = s"template or interface $tyCon"
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
