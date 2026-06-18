# \[Design - Canton BFT ordering\] Governance, phase 2, hypothesis 1: [observing][`observer` sequencer set] sequencers

## Motivation and requirements

Please refer to [phase 1] for a discussion of motivation and requirements.

This design increment concentrates in particular on `REQ-QUORUM` by defining the semantic of the
[`observer` sequencer set] sequencers set as non-voting sequencers, thus creating a direct correspondence between the
[`active` sequencer set] and the subset quorum.

This approach supports read scalability, i.e., it allows a high number of participants to read sequenced submissions.

## Design

### Basic functions and their fault tolerance

Please refer to the same section in [phase 1].

#### Canton topology: sequencer sets and functionality

Canton's sequencer topology state currently mentions two sequencer sets: [observing][`observer` sequencer set] and
[active][`active` sequencer set].
[Observing][`observer` sequencer set] sequencers are currently unimplemented, but they are meant to represent sequencers
that provide at least the read functionality.

In order to also satisfy `REQ-QUORUM`, we define the functionality of the two sequencer sets as follows:

- All sequencers, [active][`active` sequencer set] and [observing][`observer` sequencer set], support both the read and
  write (for convenience) functions.
- Only [active][`active` sequencer set] sequencers support all 3 functions (read, write and ordering).

#### Fault tolerance of each sequencer set

Let `n = a + o` be the total number of sequencers, with `a` being the total number of [active][`active` sequencer set]
sequencers and `o` being the total number of [observing][`observer` sequencer set] sequencers.

- Since [active][`active` sequencer set] sequencers support all sequencer functionality, and in particular BFT ordering,
  which is the least fault-tolerant one, the [`active` sequencer set], taken in isolation, can only tolerate
  `a_f < 1/3 a` faults on all the functionality it provides, including the read and write functions, due to the
  "whole-node fault" assumption from [phase 1].
  > Note: if the read and write functions were provided by dedicated set of sequencers, they would tolerate respectively
  > less than half and all-but-one faults.
- [observing][`observer` sequencer set] sequencers only provide the read and write functions; the
  [`observer` sequencer set], taken in isolation, can however tolerate only `o_f < 1/2 o` faults on both functions,
  still due to the "whole-node fault" assumption from [phase 1].

Members that accept trust assumptions compatible with the above fault tolerance capabilities could thus safely
utilize the read and write functions by performing reads and writes to at least _either_ `o_f + 1`
[observing][`observer` sequencer set] sequencers _or_ `a_f + 1` [active][`active` sequencer set] sequencers.

However, the next section introduces a simplification to this split model.

> [!NOTE]
>   Since a correct write is a prerequisite for a correct read, the latter require that both write and read
>   trust assumptions are satisfied.

> [!NOTE]
>   It is riskier but cheaper for a member to access the read functionality through node sets with
>   lower fault tolerance, because fewer matching reads are needed.

### Simplified sequencing and fault model

Since both [observing][`observer` sequencer set] and [active][`active` sequencer set] sequencers support the same
members-visible functionality, the members' view of the sequencing layer can be kept simple: members can still reason
and operate against a single set including all sequencers; furthermore, this unified sequencer set can overall
tolerate `a_f + o_f` read/write faults.

This simplification allows to minimize the changes to both the current Canton sequencer topology state (`ECONOMY`)
and members' sequencer client logic; in particular:

- It preserves a single threshold ([`SequencerDomainStateX.threshold`]) governed by domain owners to inform members
  about the worst-case safe sequencer read threshold, defined as `t = f + 1 = a_f + o_f + 1`.
- As a consequence, members also don't have to distinguish between different sequencer sets, which would also be
  non-trivial due to a synchronization delay between Canton topology and BFT ordering topology (as described
  in the following sections).

> [!NOTE]
>   Adopting a unified sequencer set also has a fault tolerance balancing effect for the members-visible
>   functions (i.e., read and write): if `a_f_actual < a_f`, then `o_f_actual = o_f + a_f - a_f_actual > o_f`.
>   However, `a_f_actual` can never exceed `a_f`, else ordering could be compromised, so this balancing effect
>   only works in one direction.

> [!NOTE]
>   In the future, other fault tolerance models could be considered, such as distinguishing between actively
>   malicious faults and crash faults, or a probabilistic model. These models are more expressive and
>   potentially allow for less pessimistic fault tolerance; however, they are also more complex and, in
>   particular, would require both domain owners and members to adopt a more sophisticated model of the system
>   and of its failure modes, for example by using separate thresholds for crash and byzantine
>   faults.
>
>   Luckily, since the Canton topology state doesn't mandate any specific fault tolerance model, but rather relies on
>   domain owners to assess and communicate sensible thresholds, minimal design changes are expected to switch to a
>   different fault tolerance model.

#### Updated governance examples

Here's a list of possible topology changes with examples of associated governance decisions:

- **Removing**:
  - **[Observing][`observer` sequencer set] sequencers**:
    - **Functionality**: safe because members will see read functionality being removed, on the sequencing timeline,
      before or at the same time it is actually removed (at a later epoch change).
    - **Trust assumptions and fault tolerance**:
      - Domain owners must have assessed that the observing sequencer set is safe in the new topology with the new
        members and fault tolerance.
      - They must also potentially [^1] update [`SequencerDomainStateX.threshold`] consequently; this also signals the
        new (lower) maximum safe fault tolerance to members.
  - **[Active][`active` sequencer set] sequencers**: same as for observing sequencers.
- **Adding**:
  - **[Observing][`observer` sequencer set] sequencers**:
    - **Functionality**:
      - **Reads**: the newly added sequencers are expected to serve requests from the sequencing time of addition;
        this means that, as soon as they join the BFT ordering network, they need to retrieve the consensus log
        from that timestamp on. Early reads from members, i.e. performed before the newly added sequencers have joined
        the BFT ordering P2P network [^2], will thus either be rejected with a retryable failure, or happen with
        a delay until the sequencers have joined the BFT ordering network and completed state transfer [^3].
      - **Writes**: similarly, writes performed before the newly added sequencers have joined the BFT ordering P2P
        network [^2] will either be rejected with a retryable failure or queued until the sequencers can process them
        [^3].
    - **Trust assumptions and fault tolerance**:
      - Domain owners must have assessed that the [observing][`observer` sequencer set] sequencers set is safe in the
        new topology with the new members and fault tolerance.
      - They must also potentially [^1] update [`SequencerDomainStateX.threshold`] consequently; this also signals the
        new (higher) maximum safe fault tolerance to members.
  - **[Active][`active` sequencer set] sequencers**: same as for [observing][`observer` sequencer set] sequencers.
- **Changing roles**:
  - **Turning [active][`active` sequencer set] sequencers into [observing][`observer` sequencer set] sequencers**:
    - **Functionality**: safe because the number of sequencers doesn't change and all sequencers support the
      same functionality (i.e., both read and write).
    - **Trust assumptions and fault tolerance**:
      - Domain owners must have assessed that the potentially weaker fault tolerance of the [`active` sequencer set] is
        safe.
      - Domain owners must have assessed that the [`observer` sequencer set] is safe in the new topology with the new
        members and fault tolerance.
      - They must also potentially [^1] update [`SequencerDomainStateX.threshold`] consequently; this also signals the
        new fault tolerance to members.
  - **Turning [observing][`observer` sequencer set] sequencers into [active][`active` sequencer set] sequencers**:
    similar as for turning [active][`active` sequencer set] sequencers into [observing][`observer` sequencer set]
    sequencers.

> [!NOTE]
>   The maximum safe fault tolerance corresponds to the worst case that is still surely safe; in practice,
>   domain owners may know/assess that fewer faults will realistically happen than what can be tolerated.
>   In such cases, they can e.g. use a lower [`SequencerDomainStateX.threshold`] or avoid changing it
>   when adding sequencers.
>   They may also accept the risk of running a domain where safety is not guaranteed because more faults
>   can happen than what can be tolerated, and set a higher [`SequencerDomainStateX.threshold`].

[^1]: adding or removing to the [`observer` sequencer set] will change the fault tolerance when the set grows/shrinks
  by 2 (i.e., N=3, N=5, N=7, ...); for [active][`active` sequencer set] sequencers, it would grow/shrink when membership
  changes by 3 (i.e., N=4, N=7, N=10, ...).
[^2]: in practice, this situation will be rare, because endpoints of newly added sequencers are not communicated to
  members via Canton topology, but rather out-of-band, so it will be most common for members to be able to connect to
  newly added sequencers only when the BFT ordering topology has already caught up with the Canton topology.
[^3]: an even simpler possibility is to refrain from exposing the public sequencer API until the sequencer has caught
  up and is ready to provide the functionality; this should require only minor changes to the sequencer node
  code base.
  However, when assessing fault tolerance and a worst-case safe minimum read threshold, domain owners should
  take into account the fact that the sequencer being onboarded will effectively be crash-faulty until
  it is ready to serve the public sequencer API.

# Requirements analysis (increment)

| # | Requirement mnemonic | How it is addressed                                                                                                                                                                                                                                                                                                                                                                                                                     |
|---|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 5 | **REQ-QUORUM** (new) | Sequencing functions, their BFT characteristics and their governance are explicitly defined in relation to both [active][`active` sequencer set] and [observing][`observer` sequencer set] sequencers. Governance examples are provided for adding and removing [observers][`observers` sequencer set], as well as for changing back and forth between [active][`active` sequencer set] and [observer][`observer` sequencer set] roles. |

[phase 1]: 2023Q3_007-BFT-governance-phase1.md
[`SequencerDomainStateX.threshold`]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L234
[`active` sequencer set]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L236
[`observer` sequencer set]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L238
