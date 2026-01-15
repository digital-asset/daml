// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import com.digitalasset.transcode.schema.{PackageName, PackageVersion}

package object daml_lf:
  type GenTemplateChoice[A] = com.digitalasset.daml.lf.language.Ast.GenTemplateChoice[A]

  val Ref = com.digitalasset.daml.lf.data.Ref
  val Ast = com.digitalasset.daml.lf.language.Ast
  val Util = com.digitalasset.daml.lf.language.Util

  def getMetadataDetails(
      packageSignature: Ast.PackageSignature,
      pkgId: Ref.PackageId,
  ): (PackageName, PackageVersion) =
    PackageName(packageSignature.metadata.name) -> PackageVersion(
      packageSignature.metadata.version.toString
    )

  def validatePackage(packageSignature: Ast.PackageSignature): Boolean = true
