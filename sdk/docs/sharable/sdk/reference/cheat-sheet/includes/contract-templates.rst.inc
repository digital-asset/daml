Contract Templates
------------------

Contract templates describe data that will be stored on the ledger. Templates determine who can read and write data; and by whom and how this data can be altered. A contract template is defined with the template keyword:

.. code-block::

    template MyData
      with
        i : Int
        party1 : Party
        party2 : Party
        dataKey : (Party, Text)
      where
        signatory party1
        observer  party2
        key dataKey : (Party, Text)
        maintainer key._1

        choice MyChoice : ()
        ...

``with`` and ``where`` are keywords to structure the template.

.. list-table::
   :widths: 30 70

   * - ``signatory``
     - Observes the contract and its evolution. Gives the signatory’s authority to all the defined contract updates in the contract choices.
   * - ``observer``
     - Observes the contract and its evolution.
   * - ``key``
     - A field of the contract data used as primary index of contracts defined by this template, see ``Contract Keys``.
   * - ``maintainer``
     - A set of parties that guarantee uniqueness of contract keys of this template on the ledger, see ``Contract Keys``.

