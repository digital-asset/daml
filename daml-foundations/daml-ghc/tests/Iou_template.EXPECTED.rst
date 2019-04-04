

.. _module-ioutemplate-80440:

Module Iou_template
-------------------


Templates
^^^^^^^^^

.. _template-ioutemplate-iou-86035:

template **Iou**


  .. list-table::
     :widths: 15 10 30
     :header-rows: 1
  
     * - Field
       - Type
       - Description
     * - issuer
       - Party
       -
     * - owner
       - Party
       -
     * - currency
       - Text
       - only 3-letter symbols are allowed
     * - amount
       - Decimal
       - must be positive
     * - regulators
       - [Party]
       - ``regulators`` may observe any use of the ``Iou``

  + **Choice Merge**
    merges two "compatible" ``Iou``s
  
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - otherCid
         - ContractId Iou
         - Must have same owner, issuer, and currency. The regulators may differ, and are taken from the original ``Iou``.
  + **Choice Split**
    splits into two ``Iou``s with
    smaller amounts
  
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - splitAmount
         - Decimal
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
         - Party
         -


