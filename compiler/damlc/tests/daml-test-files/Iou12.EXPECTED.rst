.. _module-iou12-32397:

Module Iou12
------------

Templates
^^^^^^^^^

.. _type-iou12-iou-45923:

**template** `Iou <type-iou12-iou-45923_>`_

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1
  
     * - Field
       - Type
       - Description
     * - issuer
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
       - 
     * - owner
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
       - 
     * - currency
       - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-57703>`_
       - only 3\-letter symbols are allowed
     * - amount
       - `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-54602>`_
       - must be positive
     * - regulators
       - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_\]
       - ``regulators`` may observe any use of the ``Iou``
  
  + **Choice Archive**
    
  
  + **Choice DoNothing**
    
  
  + **Choice Merge**
    
    merges two \"compatible\" ``Iou``s
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - otherCid
         - `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Iou <type-iou12-iou-45923_>`_
         - Must have same owner, issuer, and currency\. The regulators may differ, and are taken from the original ``Iou``\.
  
  + **Choice Split**
    
    splits into two ``Iou``s with smaller amounts
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - splitAmount
         - `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-54602>`_
         - must be between zero and original amount
  
  + **Choice Transfer**
    
    changes the owner
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - newOwner
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
         - 

Functions
^^^^^^^^^

.. _function-iou12-main-35518:

`main <function-iou12-main-35518_>`_
  \: `Scenario <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-scenario-45418>`_ ()
  
  A single test scenario covering all functionality that ``Iou`` implements\.
  This description contains a link(http://example.com), some bogus \<inline html\>,
  and words\_ with\_ underscore, to test damldoc capabilities\.
