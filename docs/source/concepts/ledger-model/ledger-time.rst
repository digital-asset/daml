.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _da-model-time:

Time in DAML
------------
Since Einstein introduced the special relativity theory, we know that time
does not exist as an absolute quantity but only in connection to space. As
such, an event is defined by the four coordinates of space and time (x,t).

We can not order two events e = (x,t) and e' = (x',t') unless they are at the 
same point x = x' or if they are causal, meaning that e.g. e was caused by e',
hence e' must have been first.

Therefore, a truly distributed ledger will exhibit the same properties: For
two transactions t1 and t2, if they unrelated, we can not order them. If one
depends on the output of the previous one, then they are causal and can be
ordered.

Consequently, we need to be very concious and careful when using time in a
distributed setting. 

The DAML interpreter provides a function getTime :: Update -> Timestamp. But
from a theoretical viewpoint, the DAML model does not provide getTime, but
another function: getSpaceTime :: Update -> SpaceTime, which returns space and
time, in conjunction with unpackTime :: SpaceTime -> Timestamp. Using above
functions, we can define getTime = getSpaceTime . unpackTime.

The consequence of this is that a series of causal DAML transactions where 
contracts read the time using getTime do not have to be ordered by the 
Timestamp, because only having the Timestamp is not sufficient information for
ordering.

Hence, the DAML ledger guarantees causality of transactions, but no total
ordering according to a single time.
