// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

import com.daml.logging.LoggingContext
import com.daml.platform.{Identifier, Party}

import scala.concurrent.{ExecutionContext, Future}

class DomainStringIterators(
    val parties: Iterator[String],
    val templateIds: Iterator[String],
)

trait InternizingStringInterningView {

  /** Internize strings of different domains. The new entries are returend as prefixed entries for persistent storage.
    *
    * @param domainStringIterators iterators of the new entires
    * @return If some of the entries were not part of the view: they will be added, and these will be returned as a interned-id and raw, prefixed string pairs.
    */
  def internize(domainStringIterators: DomainStringIterators): Iterable[(Int, String)]
}

trait UpdatingStringInterningView {

  /** Update the StringInterningView from persistence
    *
    * @param lastStringInterningId this is the "version" of the persistent view, which from the StringInterningView can see if it is behind
    * @return a completion Future: if the view is behind it will load the missing entries from persistence, and update the view state
    */
  def update(lastStringInterningId: Int)(implicit loggingContext: LoggingContext): Future[Unit]
}

/** Encapsulate the dependency to load a range of string-interning-entries from persistence
  */
trait LoadStringInterningEntries {
  def apply(
      fromExclusive: Int,
      toInclusive: Int,
  ): LoggingContext => Future[Iterable[(Int, String)]]
}

/** This uses the prefixed raw representation internally similar to the persistence layer.
  * Concurrent view usage is optimized for reading:
  * - The single, volatile reference enables non-synchronized access from all threads, accessing persistent-immutable datastructure
  * - On the writing side it synchronizes (this usage is anyway expected) and maintains the immutable internal datastructure
  */
class StringInterningView(loadPrefixedEntries: LoadStringInterningEntries)
    extends StringInterning
    with InternizingStringInterningView
    with UpdatingStringInterningView {
  @volatile private var raw: RawStringInterning = RawStringInterning.from(Nil)

  private def rawAccessor: StringInterningAccessor[String] = new StringInterningAccessor[String] {
    override def internalize(t: String): Int = raw.map(t)
    override def tryInternalize(t: String): Option[Int] = raw.map.get(t)
    override def externalize(id: Int): String = raw.idMap(id)
    override def tryExternalize(id: Int): Option[String] = raw.idMap.get(id)
  }

  private val TemplatePrefix = "t|"
  private val PartyPrefix = "p|"

  override val templateId: StringInterningDomain[Identifier] =
    StringInterningDomain.prefixing(
      prefix = TemplatePrefix,
      prefixedAccessor = rawAccessor,
      to = Identifier.assertFromString,
      from = _.toString,
    )

  override val party: StringInterningDomain[Party] =
    StringInterningDomain.prefixing(
      prefix = PartyPrefix,
      prefixedAccessor = rawAccessor,
      to = Party.assertFromString,
      from = _.toString,
    )

  override def internize(domainStringIterators: DomainStringIterators): Iterable[(Int, String)] =
    synchronized {
      val allPrefixedStrings =
        domainStringIterators.parties.map(PartyPrefix + _) ++
          domainStringIterators.templateIds.map(TemplatePrefix + _)
      val newEntries = RawStringInterning.newEntries(
        strings = allPrefixedStrings,
        rawStringInterning = raw,
      )
      updateView(newEntries)
      newEntries
    }

  override def update(
      lastStringInterningId: Int
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    if (lastStringInterningId <= raw.lastId) {
      Future.unit
    } else {
      loadPrefixedEntries(raw.lastId, lastStringInterningId)(loggingContext)
        .map(updateView)(ExecutionContext.parasitic)
    }

  private def updateView(newEntries: Iterable[(Int, String)]): Unit = synchronized {
    if (newEntries.nonEmpty) {
      raw = RawStringInterning.from(
        entries = newEntries,
        rawStringInterning = raw,
      )
    }
  }
}
