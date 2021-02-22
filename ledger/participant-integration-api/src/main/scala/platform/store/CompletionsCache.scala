package com.daml.platform.store

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties

import scala.collection.mutable

class CompletionsCache(maxElems: Int) {

  private val cache =
    mutable.Map[ApplicationId, mutable.SortedMap[Offset, CompletionStreamResponseWithParties]]()

  private val cachedOffsets = mutable.SortedMap[Offset, ApplicationId]()

  private var boundaries: Option[Boundaries] = None

  private def putInCache(
      offset: Offset,
      completion: CompletionStreamResponseWithParties,
  ): Unit = {
    val appMap = cache.getOrElseUpdate(completion.applicationId, mutable.SortedMap())
    appMap.put(offset, completion)
    cachedOffsets.put(offset, completion.applicationId)
  }

  private def removeOldest(): Unit = {
    val (oldestElemOffset, oldestElemAppId) = cachedOffsets.head
    cachedOffsets.remove(oldestElemOffset)
    cache(oldestElemAppId).remove(oldestElemOffset)
    boundaries = boundaries.map(_.copy(startExclusive = oldestElemOffset))
  }

  private def set(offset: Offset, completion: CompletionStreamResponseWithParties): Unit = {
    if (cache.size >= maxElems) {
      removeOldest()
    } else
      putInCache(offset, completion)
  }

  def set(
      startExclusive: Offset,
      endInclusive: Offset,
      completions: Seq[(Offset, CompletionStreamResponseWithParties)],
  ): Unit = {
    boundaries.foreach { boundaries =>
      if (boundaries.endInclusive < startExclusive) {
        cache.clear()
      }
    }
    boundaries = Some(Boundaries(startExclusive, endInclusive))
    completions.foreach { case (offset, completion) => set(offset, completion) }
  }

  def get(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  ): Cache = {
    val completions = cache.get(applicationId).fold[Seq[(Offset, CompletionStreamResponse)]](Nil) {
      appCompletions =>
        val rangeCompletions = appCompletions.range(startExclusive, endInclusive)
        rangeCompletions.remove(startExclusive)
        appCompletions.get(endInclusive).foreach(rangeCompletions.put(endInclusive, _))
        rangeCompletions
          .filter { case (_, completions) =>
            completions.parties.exists(parties.contains)
          }
          .toList
          .map { case (offset, completionsWithParties) =>
            (offset, completionsWithParties.completion)
          }
    }
    Cache(boundaries, completions)
  }

  def clear(): Unit = {
    cache.clear()
    cachedOffsets.clear()
    boundaries = None
  }

}

case class Boundaries(startExclusive: Offset, endInclusive: Offset)
case class Cache(boundaries: Option[Boundaries], cache: Seq[(Offset, CompletionStreamResponse)])
