.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Resource Management in Daml Application Design
##############################################

This section discusses approaches to avoiding potential resource pitfalls by strengthening Daml contract design. 

.. note::
    
    In this document, we focus on building resilience into the system by writing performant Daml code at design time. We are not concerned with ledger optimization. Read more about `Canton performance and scaling <../../canton/usermanual/performance.html>`__.