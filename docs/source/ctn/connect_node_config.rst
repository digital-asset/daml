Daml Connect Node Configuration
*******************************

Daml Connect Node can be configured to:

* run a participant service via the participant plugin and connect to a Daml Network via a remote Daml Driver

* run a domain service via the domain plugin and allow other Connect Nodes to access a Daml Network

* run both services, in which case the same Daml Connect Node is used to run a Daml Network and allow participants to join it


Some Connect Node parameters need to be defined before starting it and these parameters are :ref:`static-configuration`:
*  what Daml Drivers are available for it and how (remote or locally installed)
*  how many participant services will be running
*  on what port are the Node’s APIs goinng to be running

Other parameters are more dynamic in nature and can be configure in run-time via
the :ref:`adminstration APIs <administration-apis>` or the :ref:`console <canton-console>`:
* adding parties to the node
* connectting to a domain (or domains)

..example for a static configuration are the connectivity
..parameters to the local persistence store or the port the admin-apis should bind to. On the other hand, connecting to a domain
..or adding parties however is not a static configuration and therefore is not set via the config file but through the
..:ref:`adminstration APIs <administration_apis>`\  or the :ref:`console <canton-console>`.

.. figure:: usermanual/images/canton_node_initialization.png

.. toctree::
   :maxdepth: 2
   :hidden:
   :titlesonly:

   usermanual/static_conf.rst
   usermanual/administration.rst
   usermanual/command_line.rst
