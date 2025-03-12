// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.networking.Endpoint
import io.grpc.*
import io.grpc.NameResolver.Listener2
import io.grpc.internal.DnsNameResolverProvider

import java.net.URI
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap

/** A NameResolver that will perform DNS lookup for each of the provided hosts and aggregate the
  * results into a single resolution result.
  */
class MultiHostNameResolver(endpoints: NonEmpty[Seq[Endpoint]], args: NameResolver.Args)
    extends NameResolver {
  private val dnsNameResolverProvider = new DnsNameResolverProvider
  private val endpointResolversWithHost = endpoints.map { case endpoint @ Endpoint(host, port) =>
    val uri = URI.create(s"dns:///$host:${port.unwrap}")
    val nameResolver = dnsNameResolverProvider.newNameResolver(uri, args)
    (nameResolver, endpoint)
  }

  override def start(listener: Listener2): Unit = {
    import scala.jdk.CollectionConverters.*

    // we're going to assume for now that every dns lookup produces an address group or produces an error.
    // we may need to dig into this a bit more to see if the DNS provider will produce updated results over time
    // or if a result can indefinitely timeout.
    // we're in GRPC threading land so just use a very simple non-racy data structure for accumulating results.
    // we'll allow receiving an error to short circuit results
    type Result = Either[Status, Map[Int, List[EquivalentAddressGroup]]]
    val resultsRef = new AtomicReference[Result](Right(Map.empty))
    // ensure we only produce one result (if we receive new results for our dns lookups it won't produce a new result upstream)
    val resultsAnnounced = new AtomicBoolean(false)
    def announceOnce(fn: () => Unit): Unit =
      if (resultsAnnounced.compareAndSet(false, true)) fn()

    // wait for all results to be received before passing ours downstream
    def updateResults(update: Result => Result): Unit = {
      val results = resultsRef.updateAndGet(update(_))

      // announce result if complete
      results match {
        case Left(error) =>
          announceOnce(() => listener.onError(error))
        case Right(resolvedAddresses) =>
          // check we've collected all of them
          if (resolvedAddresses.sizeIs == endpoints.size) {
            val addresses = resolvedAddresses.values.flatten.toList
            val resolutionResult =
              NameResolver.ResolutionResult.newBuilder().setAddresses(addresses.asJava).build()
            announceOnce(() => listener.onResult(resolutionResult))
          }
      }
    }

    endpointResolversWithHost.toList.zipWithIndex.foreach { case ((resolver, endpoint), index) =>
      resolver.start(new Listener2 {
        override def onResult(resolutionResult: NameResolver.ResolutionResult): Unit = {
          // i'm pretty sure it only makes sense to result 0 or 1 address groups
          // but we'll attempt to support more in case there is some valid situation for this and is only encountered
          // in a different environment away from our tests
          val addresses =
            resolutionResult.getAddresses.asScala.toList.map { providedAddressGroup =>
              // set the ATTR_AUTHORITY_OVERRIDE attribute to ensure that this hostname is always used for certificate
              // checks against this endpoint
              new EquivalentAddressGroup(
                providedAddressGroup.getAddresses,
                Attributes
                  .newBuilder()
                  .setAll(providedAddressGroup.getAttributes)
                  .set(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE, endpoint.host)
                  // the endpoint object is passed as an attribute, which gets used by SequencerClientTokenAuthentication
                  // in order to pick the right connection to fetch tokens from for the current call
                  .set(Endpoint.ATTR_ENDPOINT, endpoint)
                  .build(),
              )
            }

          updateResults {
            case Right(results) => Right(results + (index -> addresses))
            case other => other // don't bother adding anything if we've already failed
          }
        }
        override def onError(error: Status): Unit =
          updateResults {
            // only set if we're the first error
            case Right(_) => Left(error)
            case existingError => existingError
          }
      })
    }
  }

  // default to just using the first, this should be overridden for each address group using ATTR_AUTHORITY_OVERRIDE
  override def getServiceAuthority: String = endpoints.head1.host
  override def shutdown(): Unit = endpointResolversWithHost.map(_._1).toList.foreach(_.shutdown())
}

class MultiHostNameResolverProvider extends NameResolverProvider {
  import MultiHostNameResolverProvider.scheme

  private val configs = TrieMap[String, NonEmpty[Seq[Endpoint]]]()

  def setupEndpointConfig(config: NonEmpty[Seq[Endpoint]]): String = {
    val id = UUID.randomUUID().toString
    configs.put(id, config).discard
    s"$scheme://$id"
  }

  override def isAvailable: Boolean = true
  override def priority(): Int = 5

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def newNameResolver(targetUri: URI, args: NameResolver.Args): NameResolver = {
    def mk: Option[MultiHostNameResolver] =
      configs.get(targetUri.getHost).map(new MultiHostNameResolver(_, args))

    // only kick in when we're given a matching scheme.
    // intentionally returning a null - this is java.
    Option(targetUri.getScheme).filter(_ == scheme).flatMap(_ => mk).orNull
  }

  override def getDefaultScheme: String = scheme
}

object MultiHostNameResolverProvider {
  private lazy val defaultInstance = new MultiHostNameResolverProvider

  /** This URI scheme has no significance outside of this name resolver. Is solely used so we can
    * register a config with multiple hosts and be returned a URI in the form
    * "canton-multi-host://uuid". We then know to handle this URIs within this name resolver and can
    * lookup the config using the provided uuid. Although slightly odd, I figure it's better than
    * trying to get many endpoints into a single URI to use for GRPCs `forTarget` call when building
    * a channel.
    */
  private val scheme = "canton-multi-host"

  /** GRPC will attempt to lookup resolvers from this registry based on the scheme used in target
    * uris passed to the channel builder. This registration could be done by adding a META-INF
    * service location file to the classpath. However given we sometimes get packaged in interesting
    * ways it may be simpler to just register our provider at runtime. This must be called before
    * using any multi-host uris (and ideally called only once).
    */
  def register(): Unit = NameResolverRegistry.getDefaultRegistry.register(defaultInstance)

  /** As we have many hosts it would be awfully clunky to encode them into a single URI. Instead
    * cache the hosts in memory and generate a unique id for this configuration that can be sensibly
    * put into a URI then loaded from cache if used.
    */
  def setupEndpointConfig(config: NonEmpty[Seq[Endpoint]]): String =
    defaultInstance.setupEndpointConfig(config)
}
