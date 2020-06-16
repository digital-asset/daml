.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Setting JAVA_HOME and PATH variables
####################################

Windows
*******
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Windows.

Setting the JAVA_HOME variable
==============================

1. Open ``Search`` and type "advanced system settings" and hit ``Enter``.
2. Find the ``Advanced`` tab and click on the ``Environment Variables``.
3. In the ``System variables`` section click on ``New`` if you want to set ``JAVA_HOME`` system wide. To set ``JAVA_HOME`` for a single user click on ``New`` under ``User variables``.
4. In the opened modal window for ``Variable name`` type ``JAVA_HOME`` and for the ``Variable value`` set the path to the JDK installation. Click OK once you're done.
5. Click OK and click Apply to apply the changes.

Setting the PATH variable
=========================
If you have downloaded and installed the DAML SDK using our `Windows installer <https://github.com/digital-asset/daml/releases/latest>`_ your ``PATH`` variable is already set up.

Mac OS
******
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Mac OS with ``zsh`` shell.
If you are using ``bash`` all of the instructions are quite similar, except that you will be doing all of the changes
in the ``.bash_profile`` file.

Setting the JAVA_HOME variable
==============================
Run the following command in your terminal::

        echo 'export JAVA_HOME="$(/usr/libexec/java_home)"' >> ~/.zprofile

In order for the changes to take effect you will need to restart your computer. Note that if you will be setting up the
``PATH`` variable as well you can restart your computer after you're done with all the changes. Upon restarting check that
the ``JAVA_HOME`` variable is set::

        echo $JAVA_HOME

The result should be the path to the JDK installation, something like this::

        /Library/Java/JavaVirtualMachines/jdk_version_number/Contents/Home

Setting the PATH variable
=========================
Run the following command in your terminal::

        echo 'export PATH="~/.daml/bin:$PATH"' >> ~/.zprofile

In order for the changes to take effect you will need to restart your computer. Upon restarting check the ``PATH`` variable and make sure that
the changes have been applied::

        echo $PATH

You should see ``~/.daml/bin`` in the output.

Linux
*****
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Linux for ``bash``.

Setting the JAVA_HOME variable
==============================

Java should be installed typically in a folder like ``/usr/lib/jvm/java-version``. Before running the following command
make sure to change the ``java-version`` with the actual folder found on your computer::

        echo "export JAVA_HOME=/usr/lib/jvm/java-version" >> ~/.bash_profile

In order for the changes to take effect you will need to restart your computer. Note that if you will be setting up the
``PATH`` variable as well you can restart your computer after you're done with all the changes. Upon restarting check that
the ``JAVA_HOME`` variable is set::

        echo $JAVA_HOME

The result should be the path to the JDK installation::

        /usr/lib/jvm/java-version

Setting the PATH variable
=========================

Run the following command::

        echo 'export PATH="~/.daml/bin:$PATH"' >> ~/.bash_profile

Save the file before closing.

In order for the changes to take effect you will need to restart your computer. Upon restarting check the ``PATH`` variable and make sure that
the changes have been applied::

        echo $PATH

You should see ``~/.daml/bin`` in the output.
