.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _da-model-exceptions:

Exceptions
----------

The introduction of exceptions, a new Daml feature, has many implications
for the ledger model. This page describes the changes to the ledger model
introduced as part of this new feature.

..
   SF: Once the dust settles on exceptions, these changes should be
   incorporated into the rest of the ledger model.

Structure
+++++++++

Under the new feature, Daml programs can raise and catch exceptions.
When an exception is caught, the current subtransaction is rolled
back to a certain point -- the beginning of the `try-catch` block.

To support this in our ledger model, we need to modify the transaction
structure to indicate which subtransactions were rolled back. We do this
by introducing **rollback nodes** in the transaction. Each rollback nodes
contain a rolled back subtransaction. Rollback nodes are not considered
ledger actions, since there are too many differences between rollback
nodes and .

Therefore we define transactions as a list of **nodes**, where
each node is either a ledger action or a rollback node. This is reflected
in the updated EBNF grammar for the transaction structure:

.. code-block:: none

   Transaction  ::= Node*
   Node         ::= Action | Rollback
   Rollback     ::= 'Rollback' Transaction
   Action       ::= 'Create' contract
                  | 'Exercise' party* contract Kind Transaction
                  | 'Fetch' party* contract
                  | 'NoSuchKey' key
   Kind         ::= 'Consuming' | 'NonConsuming'

Note that `Action` and `Kind` have the same definition as before, but
since `Transaction` may now contain rollback nodes, this means that an
`Exercise` action may have a rollback node as one of its consequences.
For example, ...

..
   TODO: Add example of an exercise containing a rollback node
   with diagram. 'Alice orders her bank to "withdraw 1000 USD,
   or, if it fails, to withdraw 500 USD instead".'

In addition, rollback nodes may be nested. This corresponds to a possible
case where multiple exceptions are raised and caught within the same
transaction.

Integrity
+++++++++

The notion of a ledger action coming "after" another ledger action must
be revised in the presence of rollback nodes. It is not enough to traverse
the transaction tree in prefix order, because the actions under a rollback
were rolled back.

For example, a contract may be consumed by an exercise under a rollback node,
and immediately again after the rollback node. This is allowed because the
exercise was rolled back, and this does not represent a "double spend" of
the same contract.

We define the "after" relation as a partial order, rather than a total order,
on all the actions of a transaction. This relation is defined as follows:
`act2` comes after `act1` if and only if `act2` appears after `act1` in a
pre-order traversal of the transaction tree, and any rollback nodes that
are ancestors of `act1` are also ancestors of `act2`.

With this modified "after" relation, the notion of internal consistency
remains the same. Meaning that, for any contract `c`, we still forbid the
create of `c` coming after any action on `c`, and we forbid any action on
`c` coming after a consuming exercise on `c`.

In the example of a consuming exercise in a rollback node, followed by a
consuming exercise on the same contract outside of the rollback node,
neither consuming exercise comes "after" the other. They are part of
separate continuities, one of which was rolled back, and so they don't
break the ledger integrity.

..
   TODO: Add diagrams to demonstrate the "forking".

Authorization
+++++++++++++

Since they are not ledger actions, rollback nodes do not have authorizers
directly. Instead, rollback nodes share the authorization of their children.
This is captured in the following rules:

- When a rollback node is authorized by `p`, then all of its children are
  authorized by `p`. In particular:

  - Top-level rollback nodes share the authorization of the requestors of
    the commit with all of its children.

  - Rollback nodes that are a consequence of an exercise action `act` on a
    contract `c` share the authorization of the signatories of `c` and the
    actors of `act` with all of its children.

  - A nested rollback node shares the authorization it got from its parent
    with all of its children.

- The required authorizers of a rollback node are the union of all
  the required authorizers of its children.

Privacy
+++++++

Rollback nodes also have an interesting effect on the notion of privacy in
the ledger model. When projecting a transaction for a party `p`, it's
necessary to preserve some of the rollback structure of the transaction,
even if `p` does not have the right to observe every action under it. For
example, we need `p` to be able to verify that a rolled back exercise
(to which they are an informee) is conformant, but we also need `p` to
know that the exercise was rolled back.

Before we define projection, we first need to define the simpler notion of
**raw projection**. In a raw projection for `p`, we proceed with projection
for `p` as defined in the previous sections, and when we encounter a rollback
node we replace it with the projection for `p` of all of its children.

This would be the notion of projection we would get if we did not want to
preserve any of the rollback structure. For example, the raw projection for
`p` of the "fake double spend" example would actually be a double spend, as
the rollback structure is stripped away:

..
  TODO: Add a diagram of a double spend via raw projection.

Notice that the double spend happens because the "after" relation was not
preserved during raw projection. Similarly, notice that a node that was
rolled back in the original transaction did not remain rolled back in
the raw projection. So that's two strikes against using raw projection
as our notion of projection.

If we could preserve the "after" relation, and the set of rolled back
nodes, we would have a suitable notion of projection. This observation
forms the basis for the definition that follows. Here are our goals for
defining projection in the presence of rollbacks nodes:

1. The projection for `p` should have the same set of ledger actions as the
   raw projection for `p`. More formally, the projection for `p` should
   have the same raw projection for `p` as the original transaction.

2. If `act1` and `act2` are actions in the projection for `p`, then
   `act2` comes after `act1` in the projection for `p` if and only if
   `act2` comes after `act1` in the original transaction. In other words,
   the "after" relation, when restricted to actions in the projection
   for `p`, should be preserved under projection for `p`.

3. If `act` is an action in the projection for `p`, and `act` was rolled
   back in the original transaction, then `act` is still rolled back
   in the projection for `p`. In other words, the set of rolled back
   actions, when restricted to actions in the projection for `p`, should
   be preserved under projection for `p`. We say an action is "rolled back"
   if its parent is a rollback node, or its grandparent, or its
   great-grandparent, etc.

Additionally, we do not want to leak unnecessary information when projecting
a transaction. For example, there may be many ways to generate the same
"after" relation and the same set of rolled back nodes when projecting, and
these different ways could correspond to different transactions that `p` is
not supposed to know about. To prevent this, we also introduce a privacy
requirement:

4. If two transactions share the same raw projection for `p`, and the same
   "after" relation and set of rolled back actions, when restricted to the
   actions in the raw projection for `p`, then they should have the same
   projection for `p`. Additionally, the projection for `p` should not have
   any redundant rollback nodes, it should be minimal.

Having laid out our requirements, we are now ready to define projection!
Here's the definitien:

Projection for `p` proceeds as defined in the previous sections, but when
we reach a rollback node, we first project the subtransaction contained
in the rollback node. Then, we wrap the projected subtransaction in a new
rollback node, and we try to "normalize" the result. Normalization involves
the following steps:

- If the projected rollback node starts with another rollback node, for instance:

  .. code-block:: none

    'Rollback' [ 'Rollback' tx , node1, ..., nodeN ]

  Then we re-associate the rollback nodes, bringing the inner rollback node out:

  .. code-block:: none

    'Rollback' tx, 'Rollback' [ node1, ..., nodeN ]

  We repeat this step until the projected rollback does not start with another
  rollback node.

- If the projected rollback node ends with another rollback node, for instance:

  .. code-block:: none

    'Rollback' [ node1, ..., nodeN, 'Rollback' [ node1', ..., nodeM' ] ]

  Then we flatten the inner rollback node into its parent:

  .. code-block:: none

    'Rollback' [ node1, ..., nodeN, node1', ..., nodeM' ]

- If the projected rollback node is empty, we drop it.

Note that all of these transformations preserve the "after" relation among
actions in the transaction tree, as well as the set of rolled back actions.
They only affect the structure of the transaction by reducing the amount of
variation that may occur in a projected rollback node. Thus, our privacy
requirement is satisfied, as well as our requirements to preserve the
"after" relation and the set of rolled back actions.

The privacy section of the ledger model makes a point of saying that a
contract model should be **subaction-closed** to support projections. But
this requirement is not necessarily true once we introduce rollbacks.
Rollback nodes may contain actions that are not valid as standalone actions,
since they may have been interrupted prematurely by an exception.

Instead, we require that the contract model be **projection-closed**, i.e.
closed under projections for any party 'p'. This is a weaker requirement
that matches what we actually need.

Relation to Daml Exceptions
+++++++++++++++++++++++++++

Rollback nodes are created when an exception is thrown and caught within
the same transaction. In particular, any exception that is caught within
a try-catch will generate a rollback node if there are any ledger actions
to roll back. For example:

.. code-block:: daml

   try do
     cid <- create MyContract { ... }
     exercise cid MyChoice { ... }
     throw MyException
   catch
     MyException ->
       create MyOtherContract { ... }

This Daml code will try to create a contract, and exercise a choice on this
contract, before throwing an exception. That exception is caught immediately,
and then another contract is created.

Thus a rollback node is created, to reset the ledger to the state it had
at the start of the "try" block. The rollback node contains the create and
exercise nodes. After the rollback node, another contract is created.
Thus the final transaction looks like this:

.. code-block:: none

   [
     'Rollback' [ 'Create' cid 'MyContract', 'Exercise' cid 'MyChoice' ],
     'Create' cid2 'MyOtherContract'
   ]

..
   TODO: Add diagram here instead.

Note that rollback nodes are only created if an exception is *caught*. An
uncaught exception will result in an error, not a transaction.

In addition, empty rollback nodes are not desirable, so if the generated
rollback node would contain an empty transaction, it is dropped.
