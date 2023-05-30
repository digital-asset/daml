.. _module-qualifiedinterface-53968:

QualifiedInterface
------------------

Templates
^^^^^^^^^

.. _type-qualifiedinterface-asset-82061:

**template** `Asset <type-qualifiedinterface-asset-82061_>`_

  Signatory\: issuer, owner

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
     * - amount
       - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
       -

  + **Choice** Archive

    Controller\: issuer, owner

    Returns\: ()

    (no fields)

  + **interface instance** Token **for** `Asset <type-qualifiedinterface-asset-82061_>`_
