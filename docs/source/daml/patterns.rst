.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Good design patterns
####################

Patterns have been useful in the programming world, as both a source of design inspiration, and a document of good design practices. This document is a catalog of DAML patterns intended to provide the same facility in the DA/DAML application world.

You can checkout the examples locally via ``daml new daml-patterns daml-patterns``.

:doc:`patterns/initaccept`
    The Initiate and Accept pattern demonstrates how to start a bilateral workflow. One party initiates by creating a proposal or an invite contract. This gives another party the chance to accept, reject or renegotiate.
:doc:`patterns/multiparty-agreement`
    The Multiple Party Agreement pattern uses a Pending contract as a wrapper for the Agreement contract. Any one of the signatory parties can kick off the workflow by creating a Pending contract on the ledger, filling in themselves in all the signatory fields. The Agreement contract is not created on the ledger until all parties have agreed to the Pending contract, and replaced the initiator's signature with their own.
:doc:`patterns/delegation`
    The Delegation pattern gives one party the right to exercise a choice on behalf of another party. The agent can control a contract instance on the ledger without the principal explicitly committing the action.
:doc:`patterns/authorization`
    The Authorization pattern demonstrates how to make sure a controlling party is authorized before they take certain actions.
:doc:`patterns/locking`
    The Locking pattern exhibits how to achieve locking safely and efficiently in DAML. Only the specified locking party can lock the asset through an active and authorized action. When a contract is locked, some or all choices specified on that contract may not be exercised.

.. toctree::
   :hidden:
   :maxdepth: 2

   patterns/initaccept
   patterns/multiparty-agreement
   patterns/delegation
   patterns/authorization
   patterns/locking
   patterns/legends
