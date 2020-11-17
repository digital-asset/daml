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
If you have downloaded and installed the SDK using our `Windows installer <https://github.com/digital-asset/daml/releases/latest>`_ your ``PATH`` variable is already set up.


Mac OS
******
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Mac OS with ``zsh`` shell.
If you are using ``bash`` all of the instructions are quite similar, except that you will be doing all of the changes
in the ``.bash_profile`` file.

Setting the JAVA_HOME variable
==============================
Run the following command in your terminal::

        echo 'export JAVA_HOME="$(/usr/libexec/java_home)"' >> ~/.zprofile

Setting the PATH variable
=========================
The installer will ask you and set the ``PATH`` variable for you. If you want to set your ``PATH`` variable
manually instead, run the following command in your terminal::

        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.zprofile

Verifying the changes
=====================

In order for the changes to take effect you will need to restart your computer, or, if you're using
the macOS Terminal app, you only need to quit the Terminal app (Command+Q in the Terminal window) and
reopen it. Afterward, please follow the instructions below to verify that everything was set
up correctly.

Please verify the JAVA_HOME variable by running::

        echo $JAVA_HOME

You should see the path to the JDK installation, which is something like
``/Library/Java/JavaVirtualMachines/jdk_version_number/Contents/Home``.

Next, please verify the PATH variable by running::

        echo $PATH

You should see a series of paths which includes the path to the SDK,
which is something like ``/Users/your_username/.daml/bin``.

If you do not see the changes, you may be using ``bash`` as your default shell instead of ``zsh``.
Please try these instructions again, but replace the ``~/.zprofile`` with ``~/.bash_profile`` in
the commands above.

Linux
*****
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Linux for ``bash``.

Setting the JAVA_HOME variable
==============================

Java should be installed typically in a folder like ``/usr/lib/jvm/java-version``. Before running the following command
make sure to change the ``java-version`` with the actual folder found on your computer::

        echo "export JAVA_HOME=/usr/lib/jvm/java-version" >> ~/.bash_profile

Setting the PATH variable
=========================

The installer will ask you and set the ``PATH`` variable for you. If you want to set the ``PATH`` variable
manually instead, run the following command::

        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.bash_profile

Verifying the changes
=====================

In order for the changes to take effect you will need to restart your computer. After the restart,
please follow the instructions below to verify that everything was set up correctly.

Please verify the JAVA_HOME variable by running::

        echo $JAVA_HOME

You should see the path you gave for the JDK installation, which is something like
``/usr/lib/jvm/java-version``.

Next, please verify the PATH variable by running::

        echo $PATH

You should see a series of paths which includes the path to the SDK,
which is something like ``/home/your_username/.daml/bin``.
