.. _module-constrainedclassmethod-95187:

Module ConstrainedClassMethod
-----------------------------

This module tests the case where a class method contains a constraint
not present in the class itself\.

Typeclasses
^^^^^^^^^^^

.. _class-constrainedclassmethod-a-35350:

**class** `A <class-constrainedclassmethod-a-35350_>`_ t **where**

  .. _function-constrainedclassmethod-foo-58176:
  
  `foo <function-constrainedclassmethod-foo-58176_>`_
    \: t \-\> t
  
  .. _function-constrainedclassmethod-bar-13431:
  
  `bar <function-constrainedclassmethod-bar-13431_>`_
    \: `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ t \=\> t \-\> t
