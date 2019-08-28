# <a name="module-constrainedclassmethod-95187"></a>Module ConstrainedClassMethod

This module tests the case where a class method contains a constraint
not present in the class itself.

## Typeclasses

<a name="class-constrainedclassmethod-a-35350"></a>**class** [A](#class-constrainedclassmethod-a-35350) t **where**

> <a name="function-constrainedclassmethod-foo-58176"></a>[foo](#function-constrainedclassmethod-foo-58176)
> 
> > : t -\> t
> 
> <a name="function-constrainedclassmethod-bar-13431"></a>[bar](#function-constrainedclassmethod-bar-13431)
> 
> > : [Eq](https://docs.daml.com/daml/reference/base.html#class-ghc-classes-eq-21216) t =\> t -\> t

<a name="class-constrainedclassmethod-b-99749"></a>**class** [B](#class-constrainedclassmethod-b-99749) t **where**

> <a name="function-constrainedclassmethod-baz-40143"></a>[baz](#function-constrainedclassmethod-baz-40143)
> 
> > : [B](#class-constrainedclassmethod-b-99749) b =\> b -\> t
