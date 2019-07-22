
.. _module-ioudsl-47944:

Module IouDSL
-------------


Templates
^^^^^^^^^

.. _type-ioudsl-iou-73876:

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
     * - amount
       - Decimal
       -

  + **Choice External:Archive**
  + **Choice Burn**

Template Instances
^^^^^^^^^^^^^^^^^^

.. _type-ioudsl-proposaliou-92778:

template instance **ProposalIou**
    = Proposal `Iou <type-ioudsl-iou-73876_>`_

