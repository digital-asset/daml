// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.packagemeta

import cats.implicits.*
import cats.kernel.Semigroup
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Decode
import com.daml.lf.data.{Ref, Time}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  InterfacesImplementedBy,
  VersionedTemplates,
}
import com.google.common.annotations.VisibleForTesting

final case class PackageMetadata(
    templates: VersionedTemplates = Map.empty,
    interfaces: Set[Ref.Identifier] = Set.empty,
    interfacesImplementedBy: InterfacesImplementedBy = Map.empty,
)

object PackageMetadata {
  type Priority = Time.Timestamp
  type InterfacesImplementedBy = Map[Ref.Identifier, Set[Ref.Identifier]]
  type VersionedTemplates = Map[Ref.QualifiedName, TemplatesForQualifiedName]

  final case class TemplateIdWithPriority(templateId: Ref.Identifier, priority: Priority)

  final case class TemplatesForQualifiedName(
      all: NonEmpty[Set[Ref.Identifier]],
      private[TemplatesForQualifiedName] val _primary: TemplateIdWithPriority,
  ) {
    def primary: Ref.Identifier = _primary.templateId

    def merge(other: TemplatesForQualifiedName): TemplatesForQualifiedName =
      TemplatesForQualifiedName(
        all = all ++ other.all,
        _primary =
          Ordering.by[TemplateIdWithPriority, Priority](_.priority).max(_primary, other._primary),
      )
  }

  def from(archive: DamlLf.Archive, priority: Priority): PackageMetadata = {
    val packageInfo = Decode.assertDecodeInfoPackage(archive)._2
    PackageMetadata(
      templates = createVersionedTemplatesMap(packageInfo.definedTemplates, priority),
      interfaces = packageInfo.definedInterfaces,
      interfacesImplementedBy = packageInfo.interfaceInstances,
    )
  }

  @VisibleForTesting
  private[packagemeta] def createVersionedTemplatesMap(
      definedTemplates: Set[Ref.Identifier],
      priority: Priority,
  ): Map[Ref.QualifiedName, TemplatesForQualifiedName] =
    definedTemplates.view.groupBy(_.qualifiedName).map { case (qualifiedName, templatesForQn) =>
      templatesForQn.toSeq match {
        case Seq(templateId) =>
          qualifiedName -> TemplatesForQualifiedName(
            NonEmpty(Set, templateId),
            TemplateIdWithPriority(templateId, priority),
          )
        case other =>
          throw new IllegalStateException(
            s"Expected exactly one templateId for a qualified name in a Daml archive, but got ${other.size}. This is likely a programming error. Please contact support"
          )
      }
    }

  object Implicits {
    implicit def packageMetadataSemigroup: Semigroup[PackageMetadata] =
      Semigroup.instance { case (x, y) =>
        PackageMetadata(
          templates = x.templates |+| y.templates,
          interfaces = x.interfaces |+| y.interfaces,
          interfacesImplementedBy = x.interfacesImplementedBy |+| y.interfacesImplementedBy,
        )
      }

    private implicit def templatesForQualifiedNameSemigroup: Semigroup[TemplatesForQualifiedName] =
      Semigroup.instance(_ merge _)
  }
}
