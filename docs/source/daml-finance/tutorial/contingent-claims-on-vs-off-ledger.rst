Tutorials : Contingent Claims tree on vs off ledger
###################################################

Different ways to create and store the Contingent Claims tree
*************************************************************

We have now seen two different ways of modeling a fixed coupon bond using ``Contingent Claims``:

On ledger
=========

When we use the :doc:`Derivative extension <derivative-extension>` we create the
claims tree at instrument inception and store this representation directly
on the ledger. Since the tree is stored statically it can only change if the instrument is
updated on ledger. For example, after a coupon payment a new version of the instrument (excluding
the coupon just paid) supersedes the previous version.
However, in the event of a change in a holiday calendar (which could be used to define the
coupon payment dates), the tree will not automatically change.

Off ledger
==========

When we create a :doc:`strongly typed bond instrument <contingent-claims-instrument>`
only the key parameters of the bond are stored on the ledger. The claims tree
is not, it is created on the fly, when needed (for example, in the case of lifecycling).
Consequently, if a holiday calendar changes, this will automatically impact the claims tree
the next time it is dynamically created.


Which is the preferred way?
***************************

Both options are possible, this is more a matter of personal preference. They both have
their pros and cons.

The Off ledger approach has the
advantage that it can adapt according to changes in reference data like holiday calendars.
Also, if the economic terms of the instrument would result in a very large claims tree
it could be desirable not to store it on the ledger.

On the other hand, if you need to quickly create a one-off instrument, the On ledger approach
allows you to create the claims directly from a script, without first having to define a dedicated template.
Also, if the ``Contingent Claims`` representation is actively used by both counterparties of the
trade it could be useful to have it on ledger from a transparancy point of view.
