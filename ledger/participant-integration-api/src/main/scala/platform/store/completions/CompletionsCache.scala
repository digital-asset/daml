package com.daml.platform.store.completions

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties

import scala.collection.mutable

/** In memory cache implementation for completions. If size of cache exceeds maxElems, oldest elements will be removed,
 * based on ledger offset
  * @param maxElems maximum amount of elements stored in cache
  */
private[completions] class CompletionsCache(maxElems: Int) {
  import CompletionsCache._

  private val cache =
    mutable.Map[ApplicationId, mutable.SortedMap[Offset, CompletionStreamResponseWithParties]]() // let's not do such a complicated structure unless we'll find bottleneck here //
    // consider pure functional implementation

  private val cachedOffsets = mutable.SortedMap[Offset, CompletionStreamResponseWithParties]()

  private var boundaries: Option[Boundaries] = None

  /**
   * Returns cached completions based on parameters
   * @param startExclusive
   * @param endInclusive
   * @param applicationId
   * @param parties
   * @return
   */
  def getCachedCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId, // filtering should be removed from cahce - should just return ranged cache
      parties: Set[Ref.Party],
  ): Cache = {
    val completions = cache.get(applicationId).fold[Seq[(Offset, CompletionStreamResponse)]](Nil) {
      appCompletions =>
        val rangeCompletions = appCompletions.range(startExclusive, endInclusive)
        rangeCompletions.remove(startExclusive) // moving offsets
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

  /**
   * Caches completions. This method may remove oldest completions (based on offset)
   * @param startExclusive
   * @param endInclusive
   * @param completions
   */
  def cacheCompletions(
                        startExclusive: Offset,
                        endInclusive: Offset,
                        completions: Seq[(Offset, CompletionStreamResponseWithParties)],
                      ): Unit = {
    boundaries.foreach { boundaries =>
      if (boundaries.endInclusive < startExclusive) {
        clear()
      }
    }
    boundaries = Some(Boundaries(startExclusive, endInclusive))
    completions.foreach { case (offset, completion) => set(offset, completion) }
  }

  private def putInCache(
      offset: Offset,
      completion: CompletionStreamResponseWithParties,
  ): Unit = {
    val appMap = cache.getOrElseUpdate(completion.applicationId, mutable.SortedMap())
    appMap.put(offset, completion)
    cachedOffsets.put(offset, completion)
  }

  private def removeOldest(): Unit = {
    val (oldestElemOffset, oldestElemAppId) = cachedOffsets.head
    cachedOffsets.remove(oldestElemOffset)
    cache(oldestElemAppId.applicationId).remove(oldestElemOffset)
    boundaries = boundaries.map(_.copy(startExclusive = oldestElemOffset))
  }

  private def set(offset: Offset, completion: CompletionStreamResponseWithParties): Unit = {
    if (cache.size >= maxElems) {
      removeOldest()
    } else
      putInCache(offset, completion)
  }

  private def clear(): Unit = {
    cache.clear()
    cachedOffsets.clear()
    boundaries = None
  }

}
private[completions] object CompletionsCache {
  case class Boundaries(startExclusive: Offset, endInclusive: Offset)
  case class Cache(boundaries: Option[Boundaries], cache: Seq[(Offset, CompletionStreamResponse)]) // maybe it's better to return sortedMap instead of seq + rename to rangeCache
}
