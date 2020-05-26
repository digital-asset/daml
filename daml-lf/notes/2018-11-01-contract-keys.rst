.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Contract keys design and implementation
=======================================

Timeline overview
-----------------

* DAML-LF changes (``.proto`` + spec): 1.5 man-week
* Scenario & DAML engine implementation: 1.5 man-week
* Surface DAML changes: 1 man-week
* Testing & polish: 1 week
* Around 1 month, hopefully done by end of November
* Francesco helped by Jussi / Gyorgy / LE as needed.

TODO Agree with David & Andreas & Matthias on a GSL v1 and GSL v2
timeline. The provisional timeline for GSL v1 is end of November,
assuming we can resolve the possible issue listed below (participant
node not having an ACS).

Prerequisites
-------------

* DAML Upgradability:
  <https://digitalasset.atlassian.net/browse/DEL-4760>.
* Sandbox moved to ``daml-foundations``:
  <https://digitalasset.atlassian.net/browse/DEL-5262>

The plan is to start on the design and implementation as these two
tickets complete, so that we can get ahead without waiting.

DAML-LF changes
---------------

Optional types
^^^^^^^^^^^^^^

Looking up returns 0 or 1 contract ids. We can use lists but I'd like
to just add ``Option`` to DAML-LF, since it also makes other things
nicer (e.g. SQL and JSON).

However it's not a hard requirement, we can just use lists if needed.

``Option`` would be a serializable type.

::
  ke = this lbl+ | Record ke

  lookupKey : Template t => [Party] -> Key t -> Update (Maybe (ContractId t, Value t))

DAML-LF key types definition
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We define "key types" to be serializable types, minus variants and
list.

We defensively restrict keys to types that we know can be efficiently
indexed by SQL databases as a bunch of columns.

Note: we might need to add explicit enums to DAML-LF, and add them to
key types, where an "enum" is a variant with only nullary
constructors::

  enum Foo = This | That | Whatever

Variants with only nullary constructors can be automatically compiled
to such enum types. These enum types would be compiled down to simple
strings or ints, and efficiently indexable.

If and when we add newtypes, those would also be key types.

Proto / AST changes
^^^^^^^^^^^^^^^^^^^

For each contract template, we have at most one key definition::

  message ContractKey {
    Type key_type = 1;
    Expr maintainers = 2; // key maintainers, of type [Party]
  }

  message DefTemplate {
    ...

    ContractKey contract_key = 10; // optional
  }

And we have a new update to lookup::

  message Update {
    ...
    
    message Lookup {
      TypeConName template = 1;
      Expr maintainers = 2; // of type [Party]
      Expr key = 3; // of type key_type of that template
    }
    
    message Update {
      ...
      Lookup lookup = 8; // returns Optional (ContractId t), where t is the template specified
  }

Semantics (in brief)
^^^^^^^^^^^^^^^^^^^^

A ``GlobalKey`` is defined as::

  GlobalKey = TemplateId × Set(Party) × Value

Where ``TemplateId`` is the template for the key, ``Set(Party)`` is the
set of maintainers, and ``Value`` is the key.

Each ledger then maintains a partial map::

  Keys = GlobalKey ↦ ContractId

where each contract id in the map refers to a contract of the template
id specified in its key. In a setting where participants see different
sections of the ledger, each participant obviously maintains only keys
for the part of the ledger it can see.

Moreover, a new node type is added to transactions, ``Lookup``
(together with ``Create``, ``Exercise``, and ``Fetch``), containing
the ``GlobalKey`` and the result of the lookup. An invocation of
``lookup`` results in such a node, where there result is filled in
using the contents of ``Keys``.

When committing a transaction, as we traverse it to commit it and
verify it, we add the following checks and behavior:

* For a create node for template id ``tid``, contract id ``cid``,
  maintainers ``maintainers``, and key ``key``:

  - We check that ``(tid, maintainers, key) ∉ Keys``, that is
    that the key is unique;

  - We check that ``maintainers`` is not empty, and a subset of
    the signatories, since:
    
    + The maintainers need to see the contract to be able to update
      ``Keys`` (see fetch node behavior for more details on this);
      
    + The maintainers effectively can prevent contract creation because
      of the key uniqueness check.

  - We augment ``Keys`` with ``((tid, maintainers, key), cid)``.

* An consuming exercise node results in removing ``(tid, maintainers,
  key)`` from ``Keys``, which must be present since we store keys for
  all contracts we can see.
  
* On a lookup node, we check that the committer of the transaction is
  in the maintainer set. We need to check this since we're a
  segregated ledger, and thus different nodes will contain contracts
  visible to different parties. To be sure to return ``Nothing`` only
  for contracts that indeed to not exist, we must make sure that if
  the contract *did* exist we'd see it. Therefore this limitation.

  TODO: I'm not 100% sure this is the right rule. Isn't there some other,
  more general notion of what the participant node can see? Do we need
  to tie it to the committer?

  TODO: Conversely, is it necessarily the case that a participant node
  will see everything by the committer? Ask David about these two issues.

Surface DAML changes
--------------------

Easy, ``lookup`` primitive and key + maintainers specification for templates.

DAML Engine & Scenario runner changes
-------------------------------------

1. Add a rolling ``Keys`` in the interpreter to be able to look up keys
   you've created in the same transaction;
2. Add a hash map from global keys to contract ids in the ACS, and implement
   the checks described above.

GSL v1 (Apollo) implementation
==============================

Short story: extend the ACS, add predicates embodying the steps
described in the semantics section.

Question: since the participant nodes *do not* have an ACS, can we implement
lookups that rely on ``Nothing`` being returned?

TODO ask David about the above.

GSL v2 (Sirius) implementation
==============================

TODO fill in with Andreas and Matthias

Authorizing
===========

If we have ``authorizers`` and lookup node with maintainers
``maintainers``;

1. Not authorize at all (always accept them);
   
   - Not good because it allows you to guess what keys exist, and thus
     leaks information about what contract ids are active to
     non-stakeholders.

2. ``authorizers ∩ maintainers ≠ ∅``, at least one.

   - This is a stricter condition compared to fetches, because with
     fetches we check that ``authorizers ∩ stakeholders ≠ ∅``, and we
     know that ``maintainers ⊆ stakeholders``, since ``maintainers ⊆
     signatories ⊆ stakeholders``. In other words, you won't be able
     to look up a contract by key if you're an observer but not a
     signatory.

   - However, this is problematic since lookups will induce work for
     *all* maintainers even if only a subset of the maintainers have
     authorized it, violating the tenet that nobody can be forced to
     perform work.

     To make this a bit more concrete, consider the case where a
     negative lookup is the only thing that induces a validation
     request to a maintainer who would have received the transaction
     to validate otherwise.

3. ``authorizers ⊇ maintainers``, all of them.

   - This seems to be the only safe choice for lookups, *but* note
     that for fetches which fail if the key cannot be found we can use
     the same authorization rule we use for fetch, which is much more
     lenient. The way we achieve this is that we have two DAML-LF
     primitives, ``fetchByKey`` and ``lookupByKey``, with the former
     emitting a normal fetch node, and the latter emitting a lookup
     node.

     The reason why we have a full-blown lookup node rather than a
     simple "key does not exist" node is so that the transaction
     structure is stable with what regards wrong results coming from
     the key oracle, which will happen when the user requests a key
     for a contract that is not available locally but is available
     elsewhere.

     From a more high level perspective, we want to make the
     authorization checks orthogonal to DAML-LF interpretation, which
     would not be the case if we added a "key does not exist" node as
     described above.

   - Observation by Andreas: don't we end up in the same situation if
     we have a malicious submitter node that omits the authorization
     check? For example, it could craft transactions which involve
     arbitrary parties which then will have to perform work in
     re-interpreting the transaction.

     Francesco: even if the above is true, note that if we do not have
     this auth check for ``lookupByKey`` we open ourself up to these
     sort of scenarios coming from malicious *code*, rather than
     coming from a malicious participant, which is quite a different
     threat model. (Simon agrees)

To be able to make a statement of non-existence of a key, it's clear
that we must authorize against the maintainers, and not the
stakeholders, since there are no observers to speak of.

On the other hand, when making a positive statement, we can use the
same authorization rule that we use for fetch -- that is, we check
that ``authorizers ∩ stakeholders ≠ ∅``.

Validating transactions
=======================

* Marcin: What happens when a non-maintainer runs the transaction? Do we
  just trust that the lookup result stored in the transaction is right?

  Intuitively this makes sense because we know that for the transaction
  to go through we must have it authorized by maintainers. (Simon agrees)

* Similarly, what happens when a ``fetchByKey`` is run by somebody
  who's not a maintainer? Again, we can just look at the node, and I
  think that's ok, but I'd like to check.

* The two issues above means that to *run* the DAML update in the
  validation phase we must look at the transaction as we go along.
  This seems quite a shift compared to what we do now. However, I
  think this is quite consistent with Simon's plan of having
  transaction validation to be done together with the data that you
  need to validate the transaction itself.
  
* Simon: While it's OK to just "trust" the key lookup as a non-maintainer,
  should we at least check that, if a contract was found, the computed key
  is the one we expect? This would not change the model, but would cheaply
  get rid of malicious behavior by maintainer nodes. (Francesco agrees)
