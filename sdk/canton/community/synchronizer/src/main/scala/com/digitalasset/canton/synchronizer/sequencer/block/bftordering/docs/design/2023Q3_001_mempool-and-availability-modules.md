# \[Design - Canton BFT ordering service\] Mempool and availability

## Overview

Naive mempool modules with best-effort dissemination of ordering requests have been employed in blockchains such as
Tendermint ([thesis][tendermint thesis], [paper][tendermint main paper], [light client][tendermint light client paper],
[performance evaluation][tendermint performance evaluation paper], [formal proof][tendermint formal proof paper],
[formal verification][tendermint formal verification]) and [CometBFT].

[DAG-Rider] then introduced an efficient, zero-communication asynchronous consensus primitive, using an asynchronous
block-based DAG construction, and [Narwhal and Tusk] described how to build one that offers primitives akin to
non-total Reliable Broadcast.

Narwhal is especially tailored to zero-communication asynchronous DAG consensus, of which Tusk and [Bullshark] are
well-known examples, but it also constitutes an efficient request dissemination layer with availability and
fairness guarantees that can be used for other purposes.

In particular, the Narwhal paper shows that removing dissemination from the critical path of consensus, by performing
consensus on references to already reliably-disseminated batches, can yield significantly higher throughput even
when using non-DAG consensus algorithms such as [HotStuff].

Building on these findings, [Trantor] aims to implement [ISS] while also separating request dissemination from
consensus.

The new BFT ordering service will also implement [ISS] and, similarly to Trantor, will separate request dissemination
from consensus.

This design describes how the mempool and availability modules operate as part of the BFT ordering service.

## Glossary

When describing existing systems (e.g. [Narwhal][Narwhal and Tusk]) we use the terminology proposed by the authors,
otherwise we distinguish between "batch", "ordering block" and "output block":

- A "batch" is a collection of ordering requests pulled from the mempool. Workers
  in the availability module disseminate batches prior to ordering, collecting
  enough acknowledgments from peer ordering nodes.
- An "ordering block" is a collection of batch references pulled from the availability
  layer by the ordering layer to efficiently order requests via metadata.
- An "ordered block" is an ordering block that has been already ordered by the
  ordering layer and thus appears in a specific position of the output log.
- An "output block" is an ordered block with batch references replaced by actual
  batches; it is produced by the output module and pushed to the block
  sequencer logic.

## Mempool: receiving requests

The mempool module is the entry point to the BFT ordering service from the sequencer driver.
In the first design, the availability module (described below) is solely responsible for disseminating
request data to peers. As a result, the mempool can simply accept and store requests introduced by the
local sequencer; no mempool gossip protocol (found in other ordering services) is necessary at this
point [^1].

Specifically, the mempool:

1. Queues ordering requests.
1. Produces and provides batch when asked by the availability module.

[^1]: Trantor includes both a gossiping mempool and an availability module because dissemination guarantees are
  not provided by the availability module alone, but rather jointly by both modules.

### Authorizing requests

Each ordering node is also a sequencer node, so the clients of the BFT ordering service are the sequencer clients;
furthermore, nodes only expose the sequencing API, which is authorized by Canton [^2]. Therefore, the mempool module
does not need to authenticate ordering requests in the first iteration.

[^2]: Except during onboarding a new participant node: an unauthenticated participant node can broadcast their
  onboarding request to everyone, which will then become effective. The
  [current implementation](https://github.com/DACH-NY/canton/pull/13773) does not restrict what can be sent.
  This is a foot gun that allows anyone to flood the network with garbage. In the longer term, an unauthenticated
  member will need to use a one-time token handed out by the domain governors for onboarding.

### Submitting requests in a fault-tolerant way

A number `f` of BFT ordering nodes can be arbitrarily faulty and, in particular, faulty nodes can censor requests from
clients.

For clients that exist in the same trust domain as their BFT sequencing/ordering node, they can simply submit their
request to their corresponding single node. Other clients, however, may not operate a BFT sequencing/ordering node;
for these clients to ensure that their requests are ordered, they need to submit requests to `f+1` nodes,
potentially resulting in duplication, i.e., the same request appears multiple times in the ordered request stream.

Deduplication of such requests will either be performed post-ordering, by the output module, or by Canton, i.e., above
the sequencer driver API (directly in the sequencer layer).

## Availability module: disseminating requests

The availability module is responsible for making request data available to all correct BFT ordering
nodes, such that the consensus module, while running the consensus protocol, can operate using batch references
instead of ordering batches of requests, removing a major communication overhead from the consensus protocol's critical
path (as a batch reference is typically much smaller than a batch of requests).

At the time of writing, [Narwhal][Narwhal and Tusk] is considered the state-of-the-art dissemination protocol for
high-throughput consensus implementations. Unfortunately, due to the overhead of building the DAG, latency may also
increase.

An [internal presentation about DAG protocols] is available that contains a short introduction to Narwhal.

### Reliable dissemination

Dissemination of ordering requests must be reliable against `f` arbitrarily faulty nodes. [Narwhal][Narwhal and Tusk]
ensures availability through the construction and dissemination of "availability certificates", each containing `2f+1`
signatures from distinct nodes attesting that they have validated and stored the data.

### Authenticated dissemination

The [request authorization strategy](#Authorizing-requests) for the mempool above ensures that correct ordering nodes
will not introduce maliciously-crafted ordering requests into the network. However, it does not protect correct
ordering nodes from ordering forged requests introduced by Byzantine peers that are shared via the availability
module.

On one hand, the sequencer layer should already provide support for rejecting (i.e., ignoring) ordered
requests that fail to authenticate. This allows the ordering layer's availability module to remain
simple, avoiding the need to verify signatures when processing new ordering requests.

On the other hand, it may be more convenient and efficient to reject obviously invalid ordering requests
before they can obtain availability certificates, conserving resources in both the consensus module
and sequencing layer. This additional authorization is left as a future optimization, as it requires
additional information (a signature from the originating identity) to be included alongside each
submitted ordering request.

### Fair dissemination

The availability module should ensure fair usage of networking and storage resources.

#### Networking fairness

Malicious peers in the ordering service must not be able to completely consume incoming/outgoing
bandwidth of correct peers. In practice, this requirement is met using state-of-the-art
firewall rules and rate limiting to provide protection against denial of service (DoS) attacks. The
solution likely has no specific ties with the availability module protocol.

#### Storage fairness

Malicious peers must not be able to completely consume storage resources of correct peers.
For example, with no storage fairness, malicious peers could spam ordering requests via the
availability module and force correct nodes to promise to store them (potentially indefinitely)
until disks are completely filled.

In contrast to networking fairness, storage fairness is more directly linked with the availability
module protocol. Ultimately, the availability module should provide protection for correct nodes,
e.g., through per-ordering node storage limitations and safe ordering request eviction (i.e., garbage
collection) strategies.

#### Narwhal fairness

[Narwhal]'s DAG structure inherently provides fairness and throttling:

1. Each node can produce only one block per round.
1. A node will be caught if it tries to run ahead with rounds, because every block must include
   `2f+1` proofs of availability (PoAs) from the previous round, i.e., nodes are forced to wait for
   `2f+1` nodes to contribute to a round before they can advance their round and submit another
   proposal.
1. Once a node moves to a new round, it can forget everything about past rounds and reject blocks
   with old round numbers.

With only one proposal per node in a round, one proof of availability stored for each block in a round,
one round in flight at any given time, and an upper bound on the number of batched ordering requests
references per block, correct nodes can ensure that their storage is not overwhelmed.

More generally, the [Narwhal][Narwhal and Tusk] DAG provides certain provable guarantees
with respect to ordering fairness:

* Due to the fact that at least `f+1` blocks out of `2f+1` linked from in the previous round must
  be from honest nodes, at least half of the DAG is from honest nodes.
* More than two thirds of the nodes fairly contribute to each round in the DAG.

> [!NOTE]
> The scale out version of Narwhal, according to our understanding, must provide
> an additional mechanism to ensure complete storage fairness. The scale out version disseminates
> batches of full requests and collects `2f+1` acknowledgements from peers before including
> these batch references in blocks. Proofs of availability are created only for the blocks
> and only once all referenced batches are stored, and we believe that only "lightweight" sets of
> acknowledgements are established for the individual request batches. However, caution must be
> taken to ensure that malicious peers cannot fill up the disks of correct nodes by
> spamming new request batches. In practice, nodes may want to establish some eviction or garbage
> collection process to remove storage associated with request batches that fail to appear
> in a block after some time threshold.

#### Homegrown fairness

While Narwhal's DAG structure and proofs of availability provide inherent storage fairness,
it comes at the cost of building and maintaining the graph itself. Another option
(mentioned below in the [Possible approaches](#Possible-approaches) section) is to create a
homegrown solution (similar to the availability module in [Trantor][Trantor]) that leverages
proofs of availability to reliably disseminate request data before consensus, but avoids the
need to build and maintain a DAG structure.

The problem with such a naive homegrown solution is the lack of inherent storage fairness; by not
associating proofs of availability with flow-controlled blocks, peer nodes now have the ability
to create many proofs of availability for their ordering requests, each representing a promise
that  correct nodes will continue to store the associated data. Naturally, this is a dangerous
weapon in the hands of malicious nodes that can start spamming new requests to fill up
the disks of correct nodes; the correct nodes have no option but to continue to store the
data, as they must honor their promise.

Addressing this concern requires additional safeguards. One possible approach is to have
correct nodes limit the number of unordered and unexpired proofs of availability they
are willing to participate with, enforced independently per peer ordering node. The limit
could apply to the number of request batches, the total size of request data, or even
some score that combines  multiple metrics (e.g., number of ordering requests and size
in bytes). These  independently-enforced limits ensure that correct nodes disks are not
overwhelmed.

> [!NOTE]
> Since fairness is not provided through the DAG, a weaker quorum of `f+1` signatures
> in the proof of availability is enough to guarantee that the batch can be later
> retrieved, because at least 1 signature will come from an honest node.
> However, this means that a node missing a batch may have to try asking more nodes,
> before it can successfully retrieve it.

#### Sequencing request expiration

Submissions to sequencers contain a `max_sequencing_time` field; any submission that is not
yet sequenced (and thus unavailable for execution) by their `max_sequencing_time` will not
take effect. For the BFT ordering service, this means that any ordering request that has
not been ordered by the time its `max_sequencing_time` has been reached can be garbage
collected, creating space for new requests that will actually have a chance at being
executed by Canton.

In practice, this is not necessarily straightforward to implement. Currently, ordering
requests submitted to the ordering service are treated as an opaque black box; the ordering
service does not have a way to inspect incoming requests and extract a `max_sequencing_time`
for each one. Moreover, requests are not ordered individually, but grouped into
batches for pre-ordering dissemination (proofs of availability), and then batches are
grouped into ordering blocks for the ordering protocol. With an independent per-request
`max_sequencing_time` value, how should the ordering protocol behave if only one request
(in a batch or ordering block) has timed out?

Each batch should thus include an expiration time `t` deterministically based on the contained
requests' `max_sequencing_time`.
Ordering nodes disseminating a batch indicate that it is valid for ordering until an epoch
with timestamp `>= t` starts. The proof of availability generated for this batch contains
the expiration, and thus can be checked by correct ordering nodes in the network:

* The availability module of correct nodes deterministically rejects any expired batch
  proposals.
* The ordering module of correct nodes deterministically rejects any ordering blocks that
  contain batch references (i.e., proofs of availability) that have expired.

Technically, the ordering service does not need to use submitted expirations directly:
peer ordering nodes could specify and sign their own expiration based on their own criteria.
However, this is more complicated, as the data must be provided in the proof. Nonetheless,
it is a potentially useful modification if we prefer an approach that prevents malicious
clients from choosing long expiration times for ordering requests, although a simpler
alternative is to deterministically check that the expiration information in
submissions is always within reasonable limits.

> [!NOTE]
> The starting epoch and epoch duration (for batches) and the `max_sequencing_time`
> in ordering requests must be included and accessible by the ordering module to ensure that
> correct nodes can correctly apply expirations times.

#### Garbage Collection

With expiration times available, correct nodes should employ a garbage collection
strategy to remove stored request batches that do not get ordered before that batch
expires; once expired, the contained requests, even if ordered, will not be
executed by Canton.

The expiration time of each batch, as [described above](#sequencing-request-expiration),
is a function over the individual `max_sequencing_time` assigned to each ordering
request by the client that created it [^3].

To ensure determinism, correct nodes garbage collect expired batches (and
ordering requests therein) from the availability module storage at the start of
each epoch, using the `start_of_epoch` time, which is calculated as the most recent
`end_of_epoch` timestamp + 1μs. Any batch that has yet to be ordered that has
an expiration time before the `start_of_epoch` can be removed. For the ordering
module, ordering nodes use the `start_of_epoch` time when validating proposals to
reject any ordering blocks that contain already expired ordering requests.

*Garbage collection accuracy*: by comparing the `start_of_epoch` timestamp
against expiration times of batches, it is possible that some
requests that expire during the epoch get ordered and delivered to the
sequencer. There is no correctness issue, since the sequencer already knows
how to handle (i.e., avoid executing) expired submissions delivered by an
underlying sequencer driver [^4].

A potential optimization to get better garbage collection accuracy is to use
the minimum time of an ordered block, rather than the `start_of_epoch`
time to compare against batches and timestamp expiration times. Each
ordered block is accompanied by a derivable target timestamp. The actual
timestamp assigned to the ordered block is equal or greater, depending on
whether the block's timestamp needs shifting to maintain strictly increasing
time [^5].

*Garbage collection and topology changes*: if an ordering node is removed
from the network, it may be tempting to remove stored batch requests
originating from that ordering node immediately, before those batches naturally expire.
However, this would break the availability guarantee: the ordering node may get added
back to the topology before the batches would have expired, or the ordering node may
have been faulty and colluding with another faulty ordering node that is still in the
network. In either  case, the proofs of availability for the evicted batches may
still be valid from the perspective of the ordering module. But once these proofs
get ordered, there is no way to retrieve them, leaving an unrecoverable gap in the
output log. Therefore, it is critical that all stored request batches
be maintained until they naturally expire.

Some potential optimizations to further prevent such attacks:

* Check that the expiration is within reasonable limits.
* Add a requirement in the ordering module that batches within an ordering block
  originate from the proposer of that block; this could prevent collusion scenarios
  such as the one mentioned above.
* Only acknowledge (i.e., accept) proposed ordering blocks if all referenced data
  in the batches is stored locally; this helps prevent the above attack, but also
  may come at a nontrivial performance cost.

[^3]: There are many options for such a function, such as selecting the minimum
  ordering message expiration, or the maximum, or the median. It's sensible to
  start with a simple approach, and then improve based on measurements.
[^4]: Correctness would not be affected even if valid requests were not
  ordered, since this situation would be indistinguishable from an ordering delay
  big enough for them to expire; thus, expiration accuracy is only a matter of
  efficiency.
[^5]: It is not yet clear whether this optimization has any downsides: more
  investigation is needed.

#### Write relay

Some sequencer nodes connected to the P2P network may be observing without participating
in consensus. Allowing such nodes allows the read path to scale, but it is more convenient
for clients if observing nodes are also able to relay writes to ordering nodes on their behalf.

This will be supported by allowing observing nodes to participate in the availability protocol.
Observing nodes will behave however slightly differently:

- They will build a proof of availability signed by ordering nodes only.
- They will then broadcast the proof of availability to `f+1` ordering nodes, delegating
  to them the ordering of such proofs.

In order to exclude abusive behavior by observing nodes, ordering nodes will adopt per-node
storage quotas for them too (for more details, refer to the [availability design]).

> [!NOTE]
> This two-tiered write system may result in higher duplication, since clients may
> decide to write to multiple observing nodes in order to avoid censorship.
>
> However, it makes sense for the default number of writes to be 1,
> because the probability of an SVC wanting to actively censor a participant is
> very low due to lack of incentive.

#### Duplication

Request duplication is another source of wasted bandwidth at the availability and/or
consensus level.

A source of request duplication is clients trying to avoid censorship by faulty
nodes by sending writes to `f+1` nodes. We expect [domain fees] for the Canton
domain to provide a strong incentive against clients abusing multiple writes.

Another source of request duplication are faulty or malicious nodes. This design
doesn't aim to solve the deduplication problem arising from faulty nodes, but it makes
the case that it should not cause major performance degradation in practice, because:

1. Dissemination bandwidth is not a limiting factor, rather consensus bandwidth is [^6].
1. Consensus happens on batch references, that are quite small and
   replace a big transaction volume in the consensus critical path.

However, duplication can also interfere with [domain fees], which is currently
planned to take place in the sequencer node on the read path, with a pre-check
on the write path to save resources [^7].

- If accounting is performed before the sequencer node de-duplicates requests,
  then client-originated duplication is rightfully accounted for, but node-related
  duplication, faulty or otherwise, is not.
- If accounting is performed after the sequencer node de-duplicates requests,
  then no duplication is accounted for, including client-originated duplication.

The aforementioned problems are still unsolved, but several strategies are being
explored. In particular, since [domain fees] allow limited overdraft, duplication
information could be made available after the fact.

[^6]: As shown by [Narwhal], dissemination is only limited by the available
  network bandwidth and can be easily scaled out.
[^7]: This is as not to require state-based logic execution during block
  proposal validation at the consensus level, which would make it impossible
  to effectively parallelize it.

### Supply proposals for consensus

The availability module is also responsible, upon request from the consensus module,
for producing a proposed ordering block containing one or more references to disseminated
batches.

#### Bonus: DAG consensus support

[Narwhal][Narwhal and Tusk]'s DAG structure is sufficient to achieve zero-communication
consensus. Well-known exemplars of such consensus algorithms are [Tusk][Narwhal and Tusk]
and [Bullshark].

### ISS epoch changes

Most consensus algorithms that utilize a permissioned topology model and allow for
reconfigurations proceed in epochs; each epoch uses a fixed network topology, and
reconfigurations occur between epochs.

In [ISS], each epoch has a fixed number of log slots that is known at the beginning
of the epoch. Both the epoch length and the network topology can change at epoch
boundaries.

The mempool module does not rely on knowing the endpoints and identities of the BFT
ordering nodes in the network, and is thus unaffected by [ISS] epoch changes.

The availability module, however, does rely on that knowledge, since it broadcasts
batches and checks signatures. For this reason, the consensus module needs to
notify the availability module of configuration changes, so that it can reconfigure
itself and recreate any invalidated availability certificates.

## Detailed description

### Mempool data flow(s)

- From clients: `OrderRequest(tag: bytes, payload: bytes)` (queued)
- From local availability: `CreateBatch(count)`, reply: `BatchCreated(batchId, batch)` (from queue)

### Availability data flow(s)

- From local consensus: `CreateProposal`, reply: `NewProposal(block: Seq[BatchRef])`
- From local consensus: `NewEpoch(topology, parameters)`
- From output or remote availability: `RetrieveBatches(batchRefs)`, reply: `BatchesRetrieved(batches)`

### Possible approaches

1. [Narwhal][Narwhal and Tusk] plain or scale-out
   - Pros:
     - Inherent storage fairness and rate proof of availability (PoA) rate limiting
     - Proven approach (only in DAG BFTs)
     - Good latency: [Shoal][Shoal] obtains sub-second latency by improving DAG consensus on Narwhal,
       showing that the latency bottleneck is not [Narwhal][Narwhal and Tusk] itself
   - Cons:
     - Potentially higher average latency (still need to investigate [Shoal][Shoal])
     - Significant implementation effort
     - Potentially wasted bandwidth: upon reconfiguration, the DAG would be restarted and unordered blocks
       would be thrown away (approaches that allow to extend the DAG after reconfiguration require deeper
       theoretical and practical investigation)
     - Provided fairness is overkill for non-DAG consensus; in particular, [ISS] provides ordering fairness already
     - Design risk connected with using Narwhal with non-DAG consensus, which is being experimented with but
       not thoroughly investigated in existing literature
2. Home-grown: [Narwhal][Narwhal and Tusk] PoA without DAG and round structure. Availability module
   must enforce rate limiting and storage fairness (via [limitations](#Homegrown-fairness)), but
   ordering fairness is inherently provided by ISS.
   - Pros:
     - Simpler to understand and implement than [Narwhal][Narwhal and Tusk]
     - Better latency expected w.r.t. [Narwhal][Narwhal and Tusk]
   - Cons:
     - Risk connected to novel design (sketched [above](#Homegrown-fairness))
     - Can't leverage [Narwhal][Narwhal and Tusk]'s fairness proofs
     - Pragmatic/naive mechanisms harder to prove
3. [CometBFT]-like pragmatic/naive gossiping mempool + vanilla [ISS-PBFT] (i.e., no dissemination separation)
   - Pros:
     - Best latency expected up to maximum sustainable throughput
     - No risk connected to novel design
   - Cons:
     - Significant implementation effort to create (non-trivial) robust mempool
     - Can't leverage [Narwhal][Narwhal and Tusk]'s fairness proofs
     - Lower maximum throughput (but still 80k tx/s with 500b txs and max 2048 txs/batch, according to [ISS] paper)
4. Vanilla [ISS-PBFT] (using bucket sharding of client requests)
    - Pros:
      - Best latency expected up to maximum sustainable throughput
      - No risk connected to novel design
    - Cons:
      - Risk connected to need for smarter clients
      - Sequence numbers
      - Either broadcast send from clients or smart & robust (& novel) bucket leader detection logic
      - Significant client-side implementation effort and complexity
      - Lower maximum throughput (but still 80k tx/s with 500b txs and max 2048 txs/batch, according to [ISS] paper)

[tendermint thesis]: https://knowen-production.s3.amazonaws.com/uploads/attachment/file/1814/Buchman_Ethan_201606_Msater%2Bthesis.pdf
[tendermint main paper]: https://arxiv.org/abs/1807.04938
[tendermint light client paper]: https://arxiv.org/abs/2010.07031
[tendermint formal proof paper]: https://arxiv.org/abs/1809.09858
[tendermint formal verification]: https://galois.com/blog/2021/07/formally-verifying-the-tendermint-blockchain-protocol/
[tendermint performance evaluation paper]: https://www.inf.usi.ch/pedone/Paper/2021/srds2021a.pdf
[CometBFT]: https://docs.cometbft.com/
[DAG-Rider]: https://arxiv.org/abs/2102.08325
[Narwhal and Tusk]: https://arxiv.org/abs/2105.11827
[Bullshark]: https://arxiv.org/abs/2201.05677
[HotStuff]: https://arxiv.org/abs/1803.05069
[Trantor]: https://github.com/filecoin-project/mir/tree/main/pkg/trantor
[ISS]: https://arxiv.org/abs/2203.05681
[internal presentation about DAG protocols]: https://docs.google.com/presentation/d/1x7nhyvtcV-FfPAkmuPZ_yFqbP1HJlIesXhKDmUlIFw0/edit#slide=id.g152385f32b1_0_74
[Shoal]: https://arxiv.org/pdf/2306.03058.pdf
[Domain fees]: https://docs.google.com/document/d/1_GTssleED5UPv4Ikc9GkEo56RP7g6ZgRvConUOt8uc0/edit#heading=h.j1o9vy5fqmrz
[Availability design]: 2023Q3_001_mempool-and-availability-modules.md
