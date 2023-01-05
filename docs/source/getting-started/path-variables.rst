.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Setting JAVA_HOME and PATH Variables
####################################

Windows
*******
To set up ``JAVA_HOME`` and ``PATH`` variables on Windows:

Set the JAVA_HOME Variable
==========================

1. Search for Advanced System Settings (open ``Search``, type "advanced system settings" and hit ``Enter``).
2. Find the ``Advanced`` tab and click ``Environment Variables``.
3. Click ``New`` in the ``System variables`` section (if you want to set ``JAVA_HOME`` system wide) or in the ``User variables`` section (if you want to set ``JAVA_HOME`` for a single user). This will open a modal window for ``Variable name``.
4. In the ``Variable name`` window type ``JAVA_HOME``, and for the ``Variable value`` set the path to the JDK installation. 
5. Click OK in the ``Variable name`` window.
6. Click OK in the tab and click Apply to apply the changes.

Set the PATH Variable
=====================
The ``PATH`` variable is automatically set by the `Windows installer <https://github.com/digital-asset/daml/releases/latest>`_ .


Mac OS
******

First, determine whether you are running Bash or zsh. Open a Terminal and run::

        echo $SHELL

This should return either ``/bin/bash``, in which case you are running Bash, or
``/bin/zsh``, in which case you are running zsh. 

If you get any other output, you have a non-standard setup. If you're not sure
how to set up environment variables in your setup, ask on the
`Daml forum <https://discuss.daml.com>`_ and we will be happy to help.

Open a terminal and run the following commands. Copy/paste one line at a time if possible. None of these should produce any
output on success. 

To set the variables in **bash**::

        echo 'export JAVA_HOME="$(/usr/libexec/java_home)"' >> ~/.bash_profile
        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.bash_profile

To set the variables in **zsh**::

        echo 'export JAVA_HOME="$(/usr/libexec/java_home)"' >> ~/.zprofile
        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.zprofile

For both shells, the above will update the configuration for future, newly
opened terminals, but will not affect any exsting one. 

To test the
configuration of ``JAVA_HOME`` (on either shell), open a new terminal and run::

        echo $JAVA_HOME

You should see the path to the JDK installation, which is something like ``/Library/Java/JavaVirtualMachines/jdk_version_number/Contents/Home``.

Next, please verify the ``PATH`` variable by running (again, on either shell)::

        daml version

You should see the header ``SDK versions:`` followed by a list of installed (or available) SDK versions (possibly a list of just one if you just installed).

If you do not see the expected outputs, contact us on the `Daml forum <https://discuss.daml.com>`_ and we will be happy to help.


Linux
*****
To set up ``JAVA_HOME`` and ``PATH`` variables on Linux for ``bash``:

Set the JAVA_HOME Variable
==========================

Java is typically installed in a folder like ``/usr/lib/jvm/java-version``. Before running the following command
make sure to change the ``java-version`` with the actual folder found on your computer::

        echo "export JAVA_HOME=/usr/lib/jvm/java-version" >> ~/.bash_profile

Set the PATH Variable
=====================

The installer will ask to set the ``PATH`` variable for you. If you want to set the ``PATH`` variable
manually instead, run the following command::

        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.bash_profile

Verify the Changes
==================

In order for the changes to take effect you will need to restart your computer. After the restart, verify that everything was set up correctly using the following steps:

Verify the JAVA_HOME variable by running::

        echo $JAVA_HOME

You should see the path you gave for the JDK installation, which is something like
``/usr/lib/jvm/java-version``.

Then verify the PATH variable by running::

        echo $PATH

You should see a series of paths which includes the path to the SDK,
which is something like ``/home/your_username/.daml/bin``.
