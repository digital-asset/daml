# \[Design - Canton BFT ordering\] Governance, phase 1

A high-level [introduction] to this design is available.

## Motivation

A new BFT ordering system that targets Daml 3.x is being internally developed by the Canton BFT sequencer team
in order to overcome the limitations of existing integrations with third-party blockchains (Besu, Hyperledger Fabric
and CometBFT), both in the context of Canton Network and of Canton-based consortium networks based on Daml 3.x.

In addition to overcoming functional and non-functional limitations, a dedicated BFT ordering system allows unifying
the sequencing and ordering layers and a much tighter integration with Canton’s governance and operations; this design
specifically focuses on governance, introducing the ability to also govern BFT ordering through Canton's BFT domain
topology system. In particular, it allows reusing the existing topology change authorization mechanisms, including the
support for collective governance.

# Status quo

This is a re-design of BFT ordering governance that introduces some requirements and principles, adapting other
requirements and assumptions consequently.

The [previously agreed governance design] includes pointers to relevant Canton designs, brainstorming documents
and sketches.

## Requirements

Comprehensive requirements of the new BFT ordering system can be found in [2023 BFT Consensus Requirements],
where governance-specific ones are prefixed with “Governance”.
Some non-governance requirements are also relevant for governance, e.g. node bootstrapping.

The following table defines the relevant requirements more precisely:

| # | Requirement mnemonic | Description                                                                                                                                                                                                                                                                         |
|---|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | **REQ-AUTH**         | Only relevant and properly authorized Canton entities can perform sequencing requests, read sequencer streams and issue governance requests.                                                                                                                                        |
| 2 | **REQ-WRITE**        | Canton domain owners can always reliably reconfigure the BFT ordering layer by issuing relevant BFT domain topology transactions.                                                                                                                                                   |
| 3 | **REQ-SAFE**         | Canton domain owners can always choose safe reconfiguration actions w.r.t. a correct assessment of the fault tolerance of the system.                                                                                                                                               |
| 4 | **REQ-COMM**         | Canton domain owners can always timely tell members (participants and mediators) how to safely (w.r.t. to risk assessment) use the sequencing functions.                                                                                                                            |
| 5 | **REQ-QUORUM** (new) | The design is easily extensible to support a set of ordering sequencers ("active" sequencers set or "quorum") that is strictly smaller than the set of all sequencers ("subset quorum").                                                                                            |
| 6 | **REQ-ONLINE**       | The reconfiguration of the BFT ordering layer happens without perceptible Canton domain downtime.                                                                                                                                                                                   |
| 7 | **REQ-PERF**         | The reconfiguration of the BFT ordering layer is applied without significant performance impacts; in addition, introducing governance capabilities does not have significant performance impacts on normal operation.                                                               |                                                                                                                                              |
| 8 | **REQ-TEST**         | There exists a BFT ordering service configuration and deployment with at least 4 nodes that allows, with potentially significantly reduced throughput, to achieve a latency under 1s for topology changes; this is especially useful to achieve acceptable test execution latencies. |                                                                                                                                              |

In addition to requirements, a general economy principle is now explicitly taken into account:

| # | Principle mnemonic | Description                                                            |
|---|--------------------|------------------------------------------------------------------------|
| 1 | **ECONOMY**        | The design is the most economical one that satisfies the requirements. |

## Design

### Fault tolerance

The sequencing layer provides total-order multicast and a shared time source; in a Canton domain, it is thus the
backbone of both the transaction protocol and topology management.

A BFT sequencing layer is expected to provide the same sequencing functionality with more lenient trust assumptions
w.r.t. centralized implementations; in particular, each sequencer can be operated by a separate organization [^1],
and organizations don't necessarily fully trust one another, nor a single administrating organization. That is,
in principle, each sequencer may belong to a separate "trust domain" and the remainder of this design
assumes that this most fragmented (i.e., worst-case) trust configuration is in effect.

The system as a whole should thus be able to tolerate a certain number of arbitrarily faulty trust domains, such as
trust domains affected by intentionally malicious ("byzantine") misbehavior and by unintended misbehavior, such as a
sequencer crash or an implementation bug.

[^1]: For simplicity we establish a 1:1 correspondence between organizations and trust domains: from the point of view
  of members, if multiple sequencers are operated by a single organization, they share the same Canton sequencer
  ID and are thus effectively active-active HA replicas of the same logical sequencer.

### Basic functions and their fault tolerance

Sequencing comprises several basic functions with different fault tolerance characteristics:

1. Receiving ordering requests ("write" function) from authorized members: at least one correct node must receive
   the request.
1. Streaming ordered requests to authorized members ("read" function) consistently and according to their
   order: read-serving sequencers can either be correct, crash or lie; in particular, they are excluded from
   consensus and cannot thus perform "split-brain" attacks, so a simple majority of matching reads is enough to
   confirm a correct read, i.e., `n > 2f`.
   > Note: members are however free to devise a specific strategy to this end, e.g., whether to immediately ask
   > `2f+1` sequencers, so that they are sure of getting `f+1` matching reads from the get go (of course under the
   > `n > 2f` trust assumption), or just ask `f+1` in a first attempt and request more from other sequencers if
   > those `f+1` do not match, until they obtain `f+1` matching reads.
1. Ordering and timestamping such requests consistently across a domain ("ordering/timestamping" function, for
   simplicity subsequently referred as "ordering" only): classic BFT consensus restrictions require that the total
   number of faulty BFT ordering nodes are less than one third of their total number: `n > 3f`. This function is not
   directly accessible to members, but it provides consistent ordering to the "read" function, which is necessary
   to avoid ledger forks.

These functions can, in principle, be either provided by dedicated sets of nodes, or there can be nodes that provide
more than one function.

A trust domain, which is the unity of trust and generally corresponds to an autonomous organization, can also in
principle contribute to one or more functions (through either single-function or multi-function nodes).

Since we consider malicious behavior realistic, if a trust domain contributes to a function in a faulty way,
other functions provided by it should also be considered faulty. This means, in particular, that a node
will be considered either fully correct or fully faulty, i.e., a fault may impact all the functions provided
by the faulty node ("whole-node fault" assumption).

Thus, if a node set provides more than one function, it can tolerate at most the number of faults of the *least*
fault-tolerating function it provides.

For example, let's suppose that sequencers are partitioned in two sets, write-only and read-only, and that
BFT ordering is only provided by the latter ones (i.e., write-only nodes are also "validators" or "voters",
but read-only nodes are not).

Let `r` be the total number of read nodes and `w` be the total number of write nodes: then, the system can tolerate
`r_f < 1/2 r` read faults, but only `w_f < 1/3 w` write faults. This is because:

- Nodes are either correct or faulty as a whole, so the maximum fault tolerance of the node set corresponds to the
  maximum fault tolerance of the least fault-tolerant function provided by the node set.
- Write nodes are also BFT ordering nodes.
- BFT ordering is performed through classic BFT consensus.
- The maximum fault tolerance of the write function is `f < n`, but classic BFT consensus can only tolerate at most
  `f < 1/3 n` faults. The least fault-tolerant function is thus BFT ordering, and it determines the maximum fault
  tolerance of the write node set.

### Simplified sequencing model

In phase 1, for simplicity, all sequencers will support receiving requests submission ("writes", "write function")
and streaming consistently ordered requests ("read function"), as well as actively contributing to a consistent
requests ordering ("ordering function").

Since classic BFT consensus underpins consistent ordering. Because of the "whole-node fault" assumption,
in order to guarantee safety, a sequencer set of size `N` can tolerate at most `f` faults, with `3f < N`.

### Canton governance includes BFT ordering governance

Canton topology management is a full-fledged governance system that includes support for collective governance
and is based on decentralized identities and key management.

Sets of signatories of correctly authorized topology changes that affect the whole domain are called "domain owners"
and are entrusted with crucial governance operations, such as adding and removing sequencers and mediators, as well
as recommending BFT thresholds.

This design fully preserves (`ECONOMY`) and leverages (`REQ-AUTH`, `REQ-WRITE`) for the existing Canton BFT domain
topology system.

### Authorization

Canton topology management specifies the authorization rules for topology changes.

Furthermore, access to domain services are authorized based on the topology state and, in particular, access to
sequencing core functions is only granted to registered domain members.

> [!NOTE]
>   Currently, an unauthenticated participant node can broadcast their onboarding request to everyone, which will then
>   be accepted and become effective, as checks for permissioned BFT domains are currently unimplemented and the
>   current implementation does not restrict what can be sent. This is a foot gun that allows anyone to flood the
>   network with garbage.
>
>   This will be remedied as follows: rather than unauthenticated participants being able to broadcast onboarding
>   requests, they will present a one-time token handed out by domain governors to a sequencer. The sequencer
>   itself will verify the requests and forward it to the peers. As a result, the problem of "anyone can broadcast"
>   is reduced to "malicious sequencer can broadcast".

From the perspective of the BFT ordering functionality, Canton is thus a 2-tiered system in which:

1. Registered members relay ordering requests to sequencers.
1. Sequencers have direct access to the BFT ordering network and, subject to certain rules, use it to order
   requests on behalf of members.

Within the relevant trust assumptions, since non-faulty sequencers authorize and validate both sequencing requests
and sequencing responses, and the integrity of the messages is checked, the correctness of the Canton transaction
and topology protocols are not affected by faulty sequencers, although the availability of the sequencing service
may be; in particular:

1. Faulty or abusive members may attempt to flood sequencers, resulting in denial of service
   to at least some members.
1. Faulty sequencers may also attempt to use all the available ordering bandwidth, potentially to order
   meaningless messages and deny sequencing to the whole domain.
1. When [domain fees] are implemented, faulty sequencers may replay a sequencer member's submission,
   amplifying its costs.

Scenario 1 is an extreme case of unfair behavior whose mitigation pertains to domain-level resource control and
metering, which is out of scope here.

> [!NOTE]
>   Given that an attacker will have to pay for the resources, and that "reading" will be cheaper than "writing", there
>   will be no incentive to flooding, and it's reasonable to assume that only honest submissions will happen.
>
>   Furthermore, if the domain concludes that a participant is misbehaving, the participant can be locked out by
>   domain owners through [participant deny lists].

Scenario 2 is usually dealt with by BFT consensus protocols through so-called "log (or chain) quality" guarantees,
i.e., by ensuring that at most a bounded portion of the ordering bandwidth can be subtracted by faulty nodes.
Protocols that employ naive mempool strategies may not provide such guarantees, but the Canton BFT ordering protocol
will.

Scenario 3 won't be initially prevented, but accountability of malicious behavior will be ensured, as to discourage it.

### BFT ordering governance

While Canton topology management supports a notion of delayed activation of topology changes, permissioned
blockchains and BFT ordering systems rely on a fixed network topology during each protocol run and reconfiguration
is only allowed between protocol runs. Furthermore, it is not possible to precisely predict the duration
of a protocol run.

For example, Protocol Labs’ ISS consensus algorithm (excellent [introductory video][ISS introductory video],
[paper][ISS paper]), which the current choice for the new BFT ordering, proceeds in epochs; each epoch has a fixed
length in terms of sequence numbers, rather than in terms of a precisely predictable time duration, and
reconfigurations can only be performed at the boundaries between epochs.

For this reason, the BFT ordering layer will activate sequencer topology changes with a delay; in particular,
sequencers that are newly added to the [`active` sequencer set] may become really active in consensus only at
the subsequent epoch boundary, and analogous delays apply to removals.

Thus, there is a time window between when Canton topology changes are effective and when the same changes are applied
to the BFT ordering topology, in which the expectation about functionality and trust assumptions do not match the
active topology in the BFT ordering layer.

For example, if a sequencer is added to the topology state, members could assume that it can immediately serve requests
and that the system is now more fault-tolerant, but this would not be the case until the new node is effectively
connected, caught up with the BFT ordering layer's P2P [^2] network and able to participate in ordering.

To solve this misalignment, members could consider the current sequencing layer topology state in the context
of the previous one, and assume that sequencers that were added or removed are effectively in an undefined
state in BFT ordering layer, and thus cannot be used, until they receive notice that the changes have been applied,
e.g. via a dedicated service offered by the BFT ordering layer [^3].

However, this would deviate from the current sequencer client logic and make it more complex (against `ECONOMY`).

It is simpler to rely on the current Canton topology management assumption that domain owners are responsible for
continuously assessing the risk of trust assumptions induced (among other things) by the fault tolerance
characteristics of the topology, as well as for managing the risk of changing it.

For example, the owners of a domain would only add a sequencer after having assessed that both the previous
trust assumptions and the new ones hold; namely that, during the delay period between the topology state
change and its activation in the BFT ordering layer, the new trust assumptions are also safe.
If they think that the new trust assumptions are not safe, they would either not add the sequencer, or wait
until more sequencers can be added at the same time, so that the new trust assumptions are safe.

> [!NOTE]
>   To onboard a new sequencer, since live state transfer happens via sequencers in the pre-onboarding topology,
>   trust in the safety of both topologies is also required for the duration of the synchronization delay.

Domain owners need however to be notified when the BFT ordering layer reflects a Canton topology change
("the topology at sequencing timestamp `t` is effective starting with epoch `E` that begins at sequencing
timestamp `t' >= t`"), so that they can safely perform further changes. This notification mechanism will
be provided by BFT sequencers via a dedicated service that will be implemented by the BFT sequencer Canton
subteam.

This approach allows the Canton topology system and topology management processes to stay in essence unchanged,
including bootstrap and onboarding; it also allows members to behave as they currently do, i.e., by relying solely
on the topology state to identify sequencers and to use the existing worst case minimum read threshold
[`SequencerDomainStateX.threshold`], as set by domain owners.

[^2]: P2P connectivity will use TLS for channel encryption. mTLS is currently being avoided, as there may be
  restrictions with firewalls and DoS protection services like [Cloudflare].
  The P2P network would be (at least initially) fully connected, so a new SVO would need to provide a P2P server
  certificate to other SVOs, as well as to receive their server certificates, before the new sequencer can
  start being onboarded.
  These would be regular Java-compatible TLS certificates signed by CAs (usually), and would be used only for
  channel encryption; in particular, authentication would be performed via challenge-response at the beginning
  of the P2P protocol.
  The governance design does not to specify in detail how such TLS certificates distribution happens; e.g., it
  could happen separately and out-of-band.
  However, Canton has [preliminary support for TLS distribution and TLS-based authentication] that could also
  be leveraged.

[^3]: domain owners will also need to read the BFT ordering layer status in a fault-tolerant manner.

#### Governance examples

Here's a list of possible topology changes with examples of associated governance decisions:

- **Removing sequencers**:
  - **Functionality**: safe because members will see functionality being removed, on the sequencing timeline,
    before or at the same time it is actually removed (at a later epoch change).
  - **Trust assumptions and fault tolerance**:
    - Domain owners must have assessed that the [`active` sequencer set] is safe in the new topology with the new
      members and fault tolerance.
    - They must also potentially [^4] update [`SequencerDomainStateX.threshold`] consequently; this also signals
      the new (lower) maximum fault tolerance to members.
- **Adding sequencers**:
  - **Functionality**:
    - **Reads**: the newly added sequencers are expected to serve requests from the sequencing time of addition;
      this means that, as soon as they join the BFT ordering network, they need to retrieve the consensus log
      from that timestamp on. Early reads from members, i.e. performed before the newly added sequencers have joined
      the BFT ordering P2P network [^5], will thus either be rejected with a retryable failure, or happen with
      a delay until the sequencers have joined the BFT ordering network and completed state transfer [^6].
    - **Writes**: similarly, writes performed before the newly added sequencers have joined the BFT ordering P2P
      network [^5] will either be rejected with a retryable failure or queued until the sequencers can process them [^6].
  - **Trust assumptions and fault tolerance**:
    - Domain owners must have assessed that the [`active` sequencer set] is safe in the new topology with the new members
      and fault tolerance.
    - They must also potentially [^4] update [`SequencerDomainStateX.threshold`] consequently; this also signals
      the new (higher) maximum fault tolerance to members.

> [!NOTE]
>   The maximum safe fault tolerance corresponds to the worst case that is still surely safe; in practice,
>   domain owners may know/assess that fewer faults will realistically happen than what can be tolerated.
>   In such cases, they can e.g. use a lower [`SequencerDomainStateX.threshold`] or avoid changing it
>   when adding sequencers.
>   They may also accept the risk of running a domain where safety is not guaranteed because more faults
>   can happen than what can be tolerated, and set a higher [`SequencerDomainStateX.threshold`].

[^4]: adding or removing sequencers will change the fault tolerance when the set grows/shrinks by 3
  (i.e., N=4, N=7, N=10, ...).
[^5]: in practice, this situation will be rare, because endpoints of newly added sequencers are not communicated to
  members via Canton topology, but rather out-of-band, so it will be most common for members to be able to connect to
  newly added sequencers only when the BFT ordering topology has already caught up with the Canton topology.
[^6]: an even simpler possibility is to refrain from exposing the public sequencer API until the sequencer has caught
  up and is ready to provide the functionality; this should require only minor changes to the sequencer node
  code base.
  However, when assessing fault tolerance and a worst-case safe minimum read threshold, domain owners should
  take into account the fact that the sequencer being onboarded will effectively be crash-faulty until
  it is ready to serve the public sequencer API.

### Dynamic BFT protocol configuration parameters

The topology system already supports dynamic _Canton protocol_ parameters that are meant for consumption by
participants.

Dynamic _BFT protocol_ parameters (e.g. the number of slots per ISS segment in an epoch, consensus protocol
timeouts) are only relevant for domain owners and will be supported by a new, dedicated topology transaction type.

Domain owners are responsible for providing well-formed BFT dynamic configurations, but validation could fail due to
dynamic conditions. If validation fails, the previous configuration is preserved, a warning is logged and a
notification is pushed to domain owners connected to the BFT ordering topology notification service.

> [!NOTE]
>   A possible future improvement is to reject invalid BFT ordering reconfigurations, but this requires changes to
>   the Canton topology system:
>
>   - The sequencing protocol would be changed so that a sequencing result can include a pre-validation outcome.
>   - The BFT ordering layer would inspect sequencing requests and, if a BFT reconfiguration topology transaction
>     is found, it would pre-validate the configuration and attach the outcome to the sequencing result.
>   - State Machine Replication validation of BFT reconfiguration topology transactions would need to, first of all,
>     check the pre-validation outcome from the ordering layer.

### BFT sequencer keys

BFT sequencers connect to each other forming a P2P network and need to authenticate messages via cryptographic
signatures. Hence, each sequencer must have at all times at least one active signing key tagged for use in BFT
ordering layer's P2P network.

> [!NOTE]
>   Enforcement mechanisms aren't currently provided by the topology system and are a possible future improvement.

### Canton BFT domain bootstrap with BFT sequencers

Static domain parameters, the domain owners, the initial sequencers set and the initial set of mediators
(hereafter, collectively referred as "genesis nodes") are first agreed upon; then, a typical [BFT domain bootstrap]
with BFT sequencers consists of the following high-level administrative steps:

First of all, genesis nodes start their topology manager and initialize their ID, awaiting the bootstrapping
information. They do not start the BFT layer yet [^6].

An identity and namespace delegations exchange is then carried out as follows:

1. The domain owners exchange identities (at minimum the member UIDs) out-of-band over a secure channel to produce
   a partially authorized decentralized namespace definition.
1. The partially authorized decentralized namespace definition transactions are merged and, together with the domain
   owners' namespace delegations, distributed to all nodes (domain owners, genesis sequencers, genesis mediators).
   This can be done by one node or by all nodes, depending on how the transactions are disseminated out-of-band.
1. The decentralized namespace definition and domain owners' namespace delegations are loaded by all nodes.

After these steps, the genesis nodes can successfully bootstrap the rest of the domain definition:

1. Domain owners produce a well-formed and well-authorized genesis package proposing the static domain parameters
   and the genesis topology, which includes namespace mappings, key mappings (including P2P keys), the genesis
   sequencers state (possibly including an initial dynamic BFT configuration, if the default is not adequate) and
   the genesis mediators state.
1. The genesis package is distributed out-of-band over secure channels to genesis sequencer operators.
1. Every onboarded sequencer is either configured with the P2P endpoints of all genesis sequencers,
   or they are provided as part of a manual startup procedure by their operator via admin console; endpoints
   information is distributed out-of-band over secure channels.
1. Genesis nodes are started.
1. As soon as genesis sequencers are initialized with the genesis topology, they are aware of all sequencer identities
   and key mappings, including the P2P key mappings; they can then connect to each other, forming a P2P network,
   through the endpoints information, authenticating (and associating identities to endpoints) through a
   challenge-response mechanism that is part of the P2P protocol.
1. As soon as `2f+1` genesis sequencers are P2P-connected, the BFT ordering service starts processing incoming read
   and write requests.
1. Genesis sequencer endpoints are communicated out-of-band to genesis mediator operators.
1. Genesis mediators are initialized with connections to a suitable subset of the initial sequencers.

As soon as genesis mediators have read from genesis sequencers and processed the sequenced messages corresponding to
the initial topology, the bootstrap process is complete, the domain is fully functional and participants can also
successfully request to connect.

### BFT sequencer onboarding

Once domain owners agree to onboard new sequencers:

1. Newly onboarded sequencers are started.
1. Their identity and key mappings, including P2P keys, are extracted and communicated to a domain node out-of-band
   over secure channels.
1. An existing domain node's operator uploads the identity of the newly onboarded sequencers and key mappings,
   including P2P keys, to the domain.
1. Domain owners produce, sign and submit a topology change that registers the newly onboarded sequencers.
1. Before starting, every newly onboarded sequencer is either configured with the P2P endpoints of a suitable number
   of existing sequencers, or they are provided as part of a manual startup procedure by their operator via admin
   console; endpoints information is distributed out-of-band over secure channels.

   > [!NOTE]
   >   To avoid having to communicate P2P network endpoints out-of-band, they could be published
   >   through the topology state. Some options are:
   >
   >   - Relying on domain owners to publish the P2P network endpoints as part of the dynamic BFT ordering configuration.
   >     If done correctly, and we assume that domain owners know what they are doing (i.e., no protection against
   >     validation issues is needed), then the dynamic configuration will always contain all the active P2P endpoints.
   >   - Adding support for non-key mappings to topology management; like key mappings, they would be managed autonomously
   >     by individual node operators.
   >   - Associating optional arbitrary information to existing key mappings and using the P2P one for the endpoint.
   >   - Separating the P2P key mapping into its own topology transaction type and add a network endpoint information to
   >     it.

1. Newly onboarded sequencers either queue or reject with a retryable error incoming requests from members [^6].
1. [Newly onboarded sequencers are actively initialized with the topology snapshot and the sequencer snapshot],
   connect to the configured P2P endpoints and authenticate them via challenge-response. This allows them
   to send signed requests, such as "start state transfer since my activation sequencing time", to the existing
   sequencers. If the requests are correctly signed according to active key mappings in the topology state and
   the newly onboarded sequencers are also active in the topology state, the existing sequencers will serve such
   requests but they'll not involve the newly onboarded sequencers in the consensus protocols just yet.
1. At a suitable epoch change [^6], the existing sequencers apply the new topology, connect to the newly
   onboarded sequencers and start involving them in the consensus protocols.
1. At the same suitable epoch change and as soon as they are caught up, the newly onboarded sequencers switch from
   state transfer mode to their designated protocol role.

   > [!NOTE]
   >   An alternative to considering newly onboarded sequencers as part of the ordering topology at a fixed number of
   >   epochs after their activation in the topology state, is to consider them part of the ordering topology
   >   at the first epoch boundary following them saying they are caught up.

1. The newly onboarded sequencers serve queued [^7] and new requests from members.
1. A BFT ordering topology notification containing the new BFT ordering topology is pushed to domain owners that are
   connected to the BFT ordering topology notification service.

[^6]: The eventual delay in epoch numbers w.r.t topology activation will be a network-wide
  BFT dynamic configuration parameter, and it should be chosen in such a way that newly onboarded sequencers will be
  caught up by then with very high probability.
[^7]: [This is how they already behave](https://github.com/DACH-NY/canton/blob/26d1abf0472b5ce7720d69c1ccce837588a87fa1/enterprise/domain/src/main/scala/com/digitalasset/canton/domain/sequencing/SequencerNodeX.scala#L132-L158).

## Alternatives considered

Refer to the [previously agreed governance design].

Note that this design is similar to previously discarded hypothesis 4, which had the similar aim of preserving the
simple trust and operating model of the current Canton Network Testnet, while at the same time:

1. Simplifying and harmonizing deployment and operations with Canton's, by unifying sequencer and ordering nodes.
1. Integrating and leveraging Canton's existing topology system.

# Implementation concerns

## Backwards compatibility

The change is fully backwards compatible (`ECONOMY`).

## Impact areas

The change is expected to have minor impacts, if any, on the existing Canton functionality and code base (`ECONOMY`).

### Documentation structure

Additional docs w.r.t. Daml 3.x’s operations documentation:

- Bootstrapping a Canton domain backed by the BFT ordering service.
- Operating a Canton domain backed by the BFT ordering service:
  - Trust model and assumptions.
  - Governance examples.

## Operator-facing changes

### Configuration

New driver-specific static configuration may include parameters applicable to a single sequencer, such as:

- Endpoints to allow a sequencer to connect to an existing P2P BFT ordering network.
- Deployment-specific timeouts.

### Metrics and logging

No new metrics specific to the BFT ordering governance functionality are planned and usual logging
considerations and practices apply.

### Error codes

The following new failure modes can occur and should be handled during BFT domain bootstrap:

- Canton domain bootstrap failed:
  - Insufficient constitution (it should already exist, but ordering-specific topology info also has to be mentioned).
  - BFT ordering network startup timeout.

### Data continuity

Integrated BFT ordering governance targets Daml 3.x and the 2.x -> 3.x migration will happen through a full
domain migration, so there are no data continuity concerns.

## NFR changes

### Security

Since this design leverages Canton topology management to implement BFT ordering governance, the existing topology
authentication and authorization mechanisms apply.

### High availability

Canton BFT topology supports swapping sequencers.

Transparent HA for individual sequencers should not be subject to governance and should rather be provided by
the specific BFT ordering implementation, together with associated and Canton-integrated tools and run books.

> [!NOTE]
>   With the sequencer backend unification, we'll actually have "read HA" out of the box, but will have to reuse
>   the active / passive HA for the sequencer write side. How the BFT ordering service HA looks like is TBD.

### Performance and scalability

This design does not affect scalability nor the performance of normal operation.

# Requirements analysis

| # | Requirement mnemonic | How it is addressed                                                                                                                                                                                                                                                                                                                                                                                                        |
|---|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | **REQ-AUTH**         | Satisfied by existing Canton topology and sequencing API authorization mechanisms.                                                                                                                                                                                                                                                                                                                                         |
| 2 | **REQ-WRITE**        | Satisfied by leveraging the existing Canton topology management.                                                                                                                                                                                                                                                                                                                                                           |
| 3 | **REQ-SAFE**         | Satisfied by a case-by-case analysis of functionality, trust assumptions and fault tolerance characteristics.                                                                                                                                                                                                                                                                                                              |
| 4 | **REQ-COMM**         | Satisfied by a case-by-case analysis of ([`SequencerDomainStateX.threshold`]) updates.                                                                                                                                                                                                                                                                                                                                     |
| 5 | **REQ-QUORUM** (new) | Follow-up design documents will show how the governance approach described in this design can be extended to support subset quorums.                                                                                                                                                                                                                                                                                       |
| 6 | **REQ-ONLINE**       | Satisfied by the BFT ordering layer’s support for online (although deferred) reconfiguration.                                                                                                                                                                                                                                                                                                                              |
| 7 | **REQ-PERF**         | Within the assumption that the topology client is able to quickly retrieve the newly activated topology state to be applied at epoch transitions, this design does not affect the performance of normal operation nor the scalability of the system.                                                                                                                                                                       |                                                                                                                                              |
| 8 | **REQ-TEST**         | Satisfied in a non-geographically distributed setup, because the BFT ordering service will allow very short-running epochs which, when employed with 0 or minimal Canton topology activation delay, minimizes the length of the misalignment window between Canton topology and the BFT ordering topology. Topology changes that are unrelated to sequencers (e.g. party-to-participant mappings) are not impacted at all. |                                                                                                                                              |

| # | Principle mnemonic | How it is addressed                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | **ECONOMY**        | This design iteration still satisfies the requirements but avoids, at the same time, impactful changes to Canton and the existing topology system. In particular, this design reuses the existing topology system, addressing potential periods of misalignment with the BFT ordering topology not through a brand-new (and expensive to implement) deferred topology activation mechanism, but rather via technical remedies and by exemplifying sensible governance policies. |

# Testing

## Functional test coverage

In addition to Daml 3.x-related testing, at least the following integration tests should be added (a happy path test
per top-level group is implicit):

- Bootstrapping a BFT ordering-backed Canton domain:
  - Insufficient constitution:
    - Failure due to missing P2P key reported on BFT status update service endpoint.
  - BFT ordering network bootstrap timeout (induced):
    - Due to `> f` missing / wrong endpoints.
    - Due to `> f` communication errors.
- Adding a sequencer
  - Failure due to missing P2P key reported on BFT status update service endpoint.
- Reconfiguration
  - Invalid configuration (various BFT ordering specific errors).
- Recovery from failure, misconfiguration or crash at each step within the governance processes.
  - Part of that is: given a domain node, I would like to know exactly in what state it was left when
    the failure occurred, so that I don't need to perform forensic measures when I need to resume a stuck
    onboarding procedure.

## Test coverage of performance and scalability

Performance and scalability regression tests for normal operation apply.

The same performance and scalability regression tests will in addition be run while applying various
BFT ordering service topology changes.

Additional BFT ordering reconfiguration benchmarks are a possible future improvement.

[introduction]: https://docs.google.com/presentation/d/1200OO7nyLW3CxsZXIboN1_NhqMacipcwdCT9acTpH5o
[2023 BFT Consensus Requirements]: https://docs.google.com/document/d/1rdGxqNrWg2Mnq-XSUImtudBSAY-CTesAQ-jlhKekVWY
[previously agreed governance design]: https://docs.google.com/document/d/17AfBQe4eLpcM3Jit_5dPpouPwXxM-hK6tDOuF9mgmcM
[mempool and availability design]: ../2023Q3_001_mempool-and-availability-modules.md
[2023-02 Canton Domain Governance with BFT]: https://docs.google.com/document/d/1IdE8lfuyxb9bAKbF4DcVx3tDJT06TFJ7ep6VipDFhiY/edit#heading=h.2m9ie3tp3ccg
[ISS introductory video]: https://www.youtube.com/watch?v=zhu4b88wLKE
[ISS paper]: https://arxiv.org/abs/2203.05681
[BFT domain bootstrap]: https://github.com/DACH-NY/canton/blob/834ef2d934a5f83a332858f549ffae084b105b58/enterprise/app-base/src/main/scala/com/digitalasset/canton/console/EnterpriseConsoleMacros.scala#L126
[Newly onboarded sequencers are actively initialized with the topology snapshot and the sequencer snapshot]: https://github.com/DACH-NY/canton/blob/8106fa7d640005cbcc7eb2e01c8c3b7babf7de94/enterprise/app/src/test/scala/com/digitalasset/canton/integration/OnboardsNewSequencerNodeX.scala#L119-L133
[Domain fees]: https://docs.google.com/document/d/1_GTssleED5UPv4Ikc9GkEo56RP7g6ZgRvConUOt8uc0/edit#heading=h.j1o9vy5fqmrz
[Participant deny lists]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L129
[preliminary support for TLS distribution and TLS-based authentication]: https://github.com/DACH-NY/canton/blob/b7f0ef130a5fc41f7895d748b611938e19df1518/community/base/src/main/scala/com/digitalasset/canton/topology/transaction/TopologyMapping.scala#L284-L311
[Cloudflare]: https://www.cloudflare.com/
[`SequencerDomainStateX.threshold`]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L234
[`active` sequencer set]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L236
