# \[Design - Canton BFT ordering\] Governance, phase 2, hypothesis 2: rotating quorum (DRAFT)

## Motivation and requirements

Please refer to [phase 1] for a discussion of motivation and requirements.

This design increment concentrates in particular on `REQ-QUORUM` by supporting a subset quorum via rotating quorum
approach, and it stands in alternative to [hypothesis 1 \(observers\)]; a rotating quorum approach is meant to provide
overall scalability through a configurable compromise among fault tolerance, performance and safety characteristics
of the system.

## Design

#### Rotating quorum

The idea behind rotating quorums is the following:

- In any given epoch `e`, only sequencers from a subset of size `Q <= N` participate in ordering via consensus
  ("quorum" of "validators").
- If `Q < N`, the quorum is rotated automatically by the BFT ordering layer using a strategy that is opaque
  to the rest of the system (but potentially not to the domain owners).
- Sequencers that are not part of the quorum act effectively in an observing role: they provide the read and write
  functions but do not contribute to the ordering function.

### Sequencing and fault model

Because of the rotating quorum and due to the "whole-node fault" assumption from [phase 1], as well as the fact that
classic BFT consensus underpins the ordering function, the maximum surely safe fault tolerance for read and write
is the same as for ordering, i.e., `f` such that `Q > 3f`.

Once domain owners have assessed `f`, they will thus also set the worst-case safe read threshold
[`SequencerDomainStateX.threshold`] to `f+1`.

> [!NOTE]
>   The rotating quorum approach is more powerful than [hypothesis 1 \(observers\)], because it allows to scale
>   reads while also allowing all sequencers to validate.
>
>   On the other hand, fixed size of the quorum (which, for [hypothesis 1 \(observers\)], is the
>   [`active` sequencer set]), it is also less fault-tolerant.
>
>   This is because, due to the rotating quorum, domain owners may not be able to predict whether, nor when, faults
>   occur inside or outside the quorum: if `f` faults happen to occur at the same time in a quorum with size
>   `Q <= 3f`, ordering safety may break.
>
>   For example: assuming `Q <= 3f` and a pseudo-random rotation strategy, due to [`LLN`], the empirical probability
>   of an epoch with all `f` faults simultaneously active in `Q` approaches 1 for a system that runs long enough.

#### Updated Topology model

Since, due to the rotating quorum, the topology management system cannot distinguish between
[active][`active` sequencer set] and [observing][`observer` sequencer set], the [`active` sequencer set]
in the topology state must include all sequencers in the domain and the [`observer` sequencer set] is not
needed anymore.

#### Updated governance model

The governance model should allow domain owners to tune the system for a desired balance of fault tolerance,
performance and safety. In the rotating quorum model, domain owners are able to control via topology management:

1. The [`active` sequencer set], including all rotated-in and rotated-out sequencers, of size `N`.
1. `Q`: the size of the rotating quorum [^1].
1. [`SequencerDomainStateX.threshold`]: `f+1`, where `f` is the number of simultaneously tolerated faults.

**Prioritizing Safety**

In this approach, domain owners first choose `f` and derive `Q` accordingly.

Given a "realistic" assessment of `f` maximum number of simultaneous faults, `Q` should be chosen such that
`Q > 3f`, which ensures that, even if all `f` faults are simultaneously rotated into the quorum `Q` at the same time,
ordering safety is preserved. Also, `Q` cannot be bigger than `N`.

**Prioritizing Performance and Costs**

In this approach, domain owners first choose `Q <= N` based on accepted operational costs and performance and/or
scalability expectations [^2].

A safe value for [`SequencerDomainStateX.threshold`] can be derived from `Q = 3f+1`; if such an `f` doesn't correspond
to the assessment, a different value can be chosen.

However, if an `f` is chosen such that `Q < 3f+1` to limit costs, in the worst case, ordering safety may be broken
as soon as `f` faults occur simultaneously in the rotated quorum.

Ultimately, an assessment of how many faults are possible determines the safety and the performance characteristics
of the system. A more optimistic assessment yields lower `Q`, with better performance, at the risk of breaking BFT
ordering safety as soon as too many faults occur simultaneously in the quorum.

However, if the domain owners can adequately control the quorum rotation strategy, they may be able to make sure
that too many faults can never occur in the quorum.

[^1]: domain owners can only decide the size and not directly control the quorum membership, which is determined by
  the ordering layer.
[^2]: assuming a minimum sequencer node hardware spec (including network links), the raw ordering
performance for a given reference transaction is solely dependent on the quorum size `Q` and will asymptotically
at best plateau with `Q` for most, if not all, known BFT consensus protocols; this is certainly true for
[ISS] that will be initially adopted as the consensus protocol for BFT ordering.

# Requirements analysis (increment)

| # | Requirement mnemonic | How it is addressed                                                                             |
|---|----------------------|-------------------------------------------------------------------------------------------------|
| 5 | **REQ-QUORUM** (new) | Updates to the fault, topology and governance models that support the rotating quorum approach. |

[phase 1]: 2023Q3_007-BFT-governance-phase1.md
[hypothesis 1 \(observers\)]: 2023Q4_008-BFT-governance-phase2-hypothesis1-observers.md
[ISS]: https://arxiv.org/abs/2203.05681
[`SequencerDomainStateX.threshold`]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L234
[`active` sequencer set]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L236
[`observer` sequencer set]: https://github.com/DACH-NY/canton/blob/efee2ded6b96115f3ce1f077c1bef2af018df026/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v2/topology.proto#L238
[`LLN`]: https://en.wikipedia.org/wiki/Law_of_large_numbers
