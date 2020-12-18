// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface

import java.{util => j}

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.iface.reader.Errors
import com.daml.daml_lf_dev.DamlLf

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

sealed abstract class InterfaceType extends Product with Serializable {
  def `type`: DefDataType.FWT

  def fold[Z](normal: DefDataType.FWT => Z, template: (Record.FWT, DefTemplate[Type]) => Z): Z =
    this match {
      case InterfaceType.Normal(typ) => normal(typ)
      case InterfaceType.Template(typ, tpl) => template(typ, tpl)
    }

  /** Alias for `type`. */
  def getType: DefDataType.FWT = `type`
  def getTemplate: j.Optional[_ <: DefTemplate.FWT] =
    fold({ _ =>
      j.Optional.empty()
    }, { (_, tpl) =>
      j.Optional.of(tpl)
    })
}
object InterfaceType {
  final case class Normal(`type`: DefDataType.FWT) extends InterfaceType
  final case class Template(rec: Record.FWT, template: DefTemplate[Type]) extends InterfaceType {
    def `type`: DefDataType.FWT = DefDataType(ImmArraySeq.empty, rec)
  }
}

/** The interface of a single DALF archive.  Not expressive enough to
  * represent a whole dar, as a dar can contain multiple DALF archives
  * with separate package IDs and overlapping [[QualifiedName]]s; for a
  * dar use [[EnvironmentInterface]] instead.
  */
final case class Interface(packageId: PackageId, typeDecls: Map[QualifiedName, InterfaceType]) {
  def getTypeDecls: j.Map[QualifiedName, InterfaceType] = typeDecls.asJava
}

object Interface {
  import Errors._
  import reader.InterfaceReader._

  def read(lf: DamlLf.Archive): (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) =
    readInterface(lf)

  def read(lf: (PackageId, DamlLf.ArchivePayload))
    : (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) =
    readInterface(lf)

}
