package com.daml.http
package util

import domain.ContractTypeId
import scalaz.{Applicative, \/}

sealed abstract class HasDynamicDamlValues[F[_]] {
  import HasDynamicDamlValues._

  type TmplId
  type TypeFromCtId
  type FLfV

  def traverseDamlValues[G[_]: Applicative, LfV](fa: F[LfV])(f: DynamicDamlValue[LfV] => G[OLfV]):

  def templateId(fa: F[_]): ContractTypeId.OptionalPkg

  type TypeFromCtId

  def lfType(
              fa: F[_],
              templateId: ContractTypeId.Resolved,
              f: PackageService.ResolveTemplateRecordType,
              g: PackageService.ResolveChoiceArgType,
              h: PackageService.ResolveKeyType,
            ): Error \/ TypeFromCtId
}

object HasDynamicDamlValues {
  object HasTemplateId {
    type Orig[F[_]] = HasDynamicDamlValues[F] {type TmplId <: ContractTypeId.OptionalPkg}
    type Compat[F[_]] = Aux[F, domain.LfType]
    type Aux[F[_], TFC0] = HasTemplateId[F] {type TypeFromCtId = TFC0}
  }

  sealed abstract class DynamicDamlValue[+LfV]
  object DynamicDamlValue {
    final case class TemplateRecord[+LfV](templateId: ContractTypeId.Template.Resolved, payload: LfV) extends DynamicDamlValue[LfV]
    final case class ChoiceArgument(templateId: ContractTypeId.Template.Resolved, )
    final case class TemplateKey()
  }
}