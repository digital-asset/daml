.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Participant Metering
====================

Participant metering is a way to report how many events have been submitted in a given period of time.

Daml command execution results in a Daml transaction that contains events associated with the processing of the command.

The events included in the report include:

    * Contract creation
    * Exercise of a contract (including non-consuming exercises and exercise by key)
    * Fetch of a contract (including fetch by key)
    * Lookup by contract key

Only events that originated from the local participant are included in the metering.  Events received
by the local participant from remote participants are *not* included.

Only events contained in committed transactions are included, a failed transaction has no effect on ledger metering.

Generate a Metering Report
--------------------------

A metering report is generated using the :doc:`Daml assistant </tools/assistant>` utility.

To run a metering report ``daml ledger metering-report`` is used with the following metering specific arguments:

:``--from``:
    A start date that is used to initiate the reporting period. Events on or after this date will be included.

:``--to``:
    An end date that may be used to terminate the reporting period.  Events prior to this date will be included.
    If an end date is not provided then the report will contain counts of all events that occurred on or after
    the ``--from`` date.

:``--application``:
    Optionally, provide an application to limit the report to that application.

The from and to dates above should be formatted ``yyyy-mm-dd``.  The exact timestamp used for the report
will be the start of the UTC day provided.

Ledger metering is not affected by participant pruning.

Other non-metering specific Daml assistant flags may also be used alongside those shown above.

Example
-------

To report on all applications for January 2022 the following from/to flags would be set:

``daml ledger metering-report --from 2022-01-01 --to 2022-02-01``


Output
------

.. code-block:: json

    {
        "participant": "some-participant",
        "request": {
            "from": "2022-01-01T00:00:00Z",
            "to": "2022-02-01T00:00:00Z"
        },
        "final": true,
        "applications": [
            {
                "application": "some-application",
                "events": 42
            }
        ]
    }

The output consists of the following sections:

:participant:
    The name of the local participant the report applies to

:request:
    This section gives details of the parameters that were used to generate the report

:final:
    This field will be set to ``true`` if a ``--to`` date was provided and the ``--to`` date is
    in the past.  Once a report is marked as final the event counts will never change and so
    may be used for billing purposes.

:applications:
    This section will give an event count for each application used in the reporting period.




