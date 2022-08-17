// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.packagemeta

import com.daml.lf.data.Ref

final case class PackageMetadata private[packagemeta] (
    templates: Set[Ref.Identifier] = Set.empty,
    interfaces: Set[Ref.Identifier] = Set.empty,
    interfacesImplementedBy: Map[Ref.Identifier, Set[Ref.Identifier]] = Map.empty,
) {
  def append(
      updated: PackageMetadata
  ): PackageMetadata =
    PackageMetadata(
      templates = templates ++ updated.templates,
      interfaces = interfaces ++ updated.interfaces,
      interfacesImplementedBy = updated.interfacesImplementedBy.foldLeft(interfacesImplementedBy) {
        case (acc, (interface, templates)) =>
          acc + (interface -> (acc.getOrElse(interface, Set.empty) ++ templates))
      },
    )
}
