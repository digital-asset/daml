.. _module-iou12-76192:

Iou12
-----

Templates
^^^^^^^^^

.. _type-iou12-iou-72962:

**template** `Iou <type-iou12-iou-72962_>`_

  Signatory\: issuer

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1

     * - Field
       - Type
       - Description
     * - issuer
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
       -
     * - owner
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
       -
     * - currency
       - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
       - only 3\-letter symbols are allowed
     * - amount
       - `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135>`_
       - must be positive
     * - regulators
       - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
       - ``regulators`` may observe any use of the ``Iou``

  + **Choice** Archive

    Controller\: issuer

    Returns\: ()

    (no fields)

  + .. _type-iou12-donothing-75627:

    **Choice** `DoNothing <type-iou12-donothing-75627_>`_

    Controller\: owner

    Returns\: ()

    (no fields)

  + .. _type-iou12-merge-98901:

    **Choice** `Merge <type-iou12-merge-98901_>`_

    merges two \"compatible\" ``Iou``s

    Controller\: owner

    Returns\: `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - otherCid
         - `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_
         - Must have same owner, issuer, and currency\. The regulators may differ, and are taken from the original ``Iou``\.

  + .. _type-iou12-split-33517:

    **Choice** `Split <type-iou12-split-33517_>`_

    splits into two ``Iou``s with smaller amounts

    Controller\: owner

    Returns\: (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_)

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - splitAmount
         - `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135>`_
         - must be between zero and original amount

  + .. _type-iou12-transfer-99339:

    **Choice** `Transfer <type-iou12-transfer-99339_>`_

    changes the owner

    Controller\: owner

    Returns\: `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - newOwner
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -

Functions
^^^^^^^^^

.. _function-iou12-updateowner-56091:

`updateOwner <function-iou12-updateowner-56091_>`_
  \: `Iou <type-iou12-iou-72962_>`_ \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_)

.. _function-iou12-updateamount-41005:

`updateAmount <function-iou12-updateamount-41005_>`_
  \: `Iou <type-iou12-iou-72962_>`_ \-\> `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Iou <type-iou12-iou-72962_>`_)

.. _function-iou12-main-28537:

`main <function-iou12-main-28537_>`_
  \: Script ()

  A single test case covering all functionality that ``Iou`` implements\.
  This description contains a link(http://example.com), some bogus \<inline html\>,
  and words\_ with\_ underscore, to test damldoc capabilities\.
