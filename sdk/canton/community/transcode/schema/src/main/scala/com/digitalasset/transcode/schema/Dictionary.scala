// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import scala.annotation.{static, varargs}

/** Dictionary of templates, keys, choice arguments and choice results. */
final case class Dictionary[T](entities: Seq[Template[T]], strictPackageMatching: Boolean = false) {
  def getTemplate(templateId: Identifier): Option[T] =
    templates.get(entityKey(templateId))
  def template(templateId: Identifier): T =
    templates(entityKey(templateId))
  def getTemplateKey(templateId: Identifier): Option[T] =
    templateKeys.get(entityKey(templateId))
  def templateKey(templateId: Identifier): T =
    templateKeys(entityKey(templateId))
  def getChoiceArgument(templateId: Identifier, choiceName: ChoiceName): Option[T] =
    choiceArguments.get(choiceKey(templateId, choiceName))
  def choiceArgument(templateId: Identifier, choiceName: ChoiceName): T =
    choiceArguments(choiceKey(templateId, choiceName))
  def getChoiceResult(templateId: Identifier, choiceName: ChoiceName): Option[T] =
    choiceResults.get(choiceKey(templateId, choiceName))
  def choiceResult(templateId: Identifier, choiceName: ChoiceName): T =
    choiceResults(choiceKey(templateId, choiceName))
  def payloads: Seq[T] =
    entities.flatMap(e =>
      Seq(e.payload) ++ e.key ++ e.choices.flatMap(c => Seq(c.argument, c.result))
    )

  def map[U](f: T => U): Dictionary[U] =
    copy(entities = entities.map(_.map(f)))

  def filterIdentifiers(filter: Identifier => Boolean): Dictionary[T] =
    copy(entities = entities.filter(x => filter(x.templateId)))

  def zip[U](other: Dictionary[U]): Dictionary[(T, U)] =
    zipWith(other)((_, _))

  def zipWith[U, V](other: Dictionary[U])(combine: (T, U) => V): Dictionary[V] =
    val matching = this.strictPackageMatching || other.strictPackageMatching // use stricter rule
    val thisDictionary = this.useStrictPackageMatching(matching)
    val thatDictionary = other.useStrictPackageMatching(matching)
    val thisMap =
      thisDictionary.entities.map(t => thisDictionary.entityKey(t.templateId) -> t).toMap
    val thatMap =
      thatDictionary.entities.map(t => thatDictionary.entityKey(t.templateId) -> t).toMap
    val keys = thisMap.keySet intersect thatMap.keySet
    Dictionary(keys.toSeq.map(id => thisMap(id).zipWith(thatMap(id))(combine)), matching)

  def useStrictPackageMatching(flag: Boolean): Dictionary[T] =
    if strictPackageMatching == flag then this else copy(strictPackageMatching = flag)
  def matchByPackageId: Dictionary[T] = useStrictPackageMatching(true)
  def matchByPackageName: Dictionary[T] = useStrictPackageMatching(false)

  private lazy val templates: Map[EntityKey, T] =
    (for (t <- entities) yield entityKey(t.templateId) -> t.payload).toMap
  private lazy val templateKeys: Map[EntityKey, T] =
    (for (t <- entities; k <- t.key) yield entityKey(t.templateId) -> k).toMap
  private lazy val choiceArguments: Map[ChoiceKey, T] =
    (for (t <- entities; c <- t.choices) yield choiceKey(t.templateId, c.name) -> c.argument).toMap
  private lazy val choiceResults: Map[ChoiceKey, T] =
    (for (t <- entities; c <- t.choices) yield choiceKey(t.templateId, c.name) -> c.result).toMap

  private type EntityKey = (PackageId | PackageName, ModuleName, EntityName)
  private def entityKey(id: Identifier): EntityKey =
    if strictPackageMatching
    then (id.packageId, id.moduleName, id.entityName)
    else (id.packageName, id.moduleName, id.entityName)
  private type ChoiceKey = (PackageId | PackageName, ModuleName, EntityName, ChoiceName)
  private def choiceKey(id: Identifier, choiceName: ChoiceName) =
    if strictPackageMatching
    then (id.packageId, id.moduleName, id.entityName, choiceName)
    else (id.packageName, id.moduleName, id.entityName, choiceName)
}

object Dictionary {
  def make[T](entities: Template[T]*): Dictionary[T] = new Dictionary(entities)

  @static
  class Builder[T] {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var buf = List.empty[Template[T]]
    def addTemplate(id: Identifier, payload: T): Builder[T] = {
      buf = Template(id, payload, None, false, Seq.empty, Seq.empty) :: buf
      this
    }
    def asInterface(): Builder[T] = replaceLast { last =>
      last.copy(isInterface = true)
    }
    @varargs def implementing(ids: Identifier*): Builder[T] = replaceLast { last =>
      last.copy(implements = last.implements ++ ids)
    }
    def withKey(key: T): Builder[T] = replaceLast { last =>
      last.copy(key = Some(key))
    }
    def withChoice(name: String, consuming: Boolean, argument: T, result: T): Builder[T] =
      replaceLast { last =>
        last.copy(choices = last.choices :+ Choice(ChoiceName(name), consuming, argument, result))
      }

    def build: Dictionary[T] = Dictionary(buf.reverse)

    private def replaceLast(modify: Template[T] => Template[T]) = {
      val head :: tail = buf: @unchecked
      buf = modify(head) :: tail
      this
    }

  }
}
