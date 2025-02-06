// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2.damldefinitionsservice

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.{
  AllTemplatesResponse,
  TemplateDefinition,
  TypeSig,
}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.daml.lf.data.Ref

class DamlDefinitionsView(
    getPackageMetadataSnapshot: ContextualizedErrorLogger => PackageMetadata
) {

  def packageSignature(pkgId: String): Option[TypeSig] = {
    val validatedPackageId = Ref.PackageId.fromString(pkgId).getOrElse {
      throw new IllegalArgumentException(s"TODO(#21695): Invalid package ID: $pkgId")
    }
    val packageSignatures = getPackageMetadataSnapshot(NoLogging).packages
    packageSignatures.get(validatedPackageId).map { astPkg =>
      DamlDefinitionsBuilders.buildTypeSig(validatedPackageId, astPkg, packageSignatures)
    }
  }

  def templateDefinition(templateId: String): Option[TemplateDefinition] = {
    val validatedTemplateId = Ref.Identifier.fromString(templateId).getOrElse {
      throw new IllegalArgumentException(s"TODO(#21695): Invalid template ID: $templateId")
    }

    val packageSignatures = getPackageMetadataSnapshot(NoLogging).packages
    DamlDefinitionsBuilders.buildTemplateDefinition(validatedTemplateId, packageSignatures)
  }

  def allTemplates(): AllTemplatesResponse =
    AllTemplatesResponse(
      // TODO(#21695): Extract and use a ContextualizedErrorLogger
      getPackageMetadataSnapshot(NoLogging).templates
    )
}
