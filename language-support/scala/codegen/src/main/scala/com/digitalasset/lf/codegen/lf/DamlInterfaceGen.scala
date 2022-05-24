package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.codegen.lf.LFUtil
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{Identifier, QualifiedName}
import com.daml.lf.iface
import com.typesafe.scalalogging.Logger
import scalaz.syntax.std.option._

import scala.reflect.runtime.universe._

object DamlInterfaceGen {
  def generate(
      util: LFUtil,
      templateId: Identifier,
      interfaceSignature: iface.DefInterface.FWT,
      companionMembers: Iterable[Tree],
  ): (File, Set[Tree], Iterable[Tree]) = {}
}
