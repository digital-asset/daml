.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

First, you need to figure out whether you are running Bash or zsh. To do that, open a Terminal and run::

        echo $SHELL

This should return either ``/bin/bash``, in which case you are running Bash, or
``/bin/zsh``, in which case you are running zsh. We provide instructions for
both, but you only need to follow the instructions for the one you are using.

If you get any other output, you have a non-standard setup. If you're not sure
how to set up environment variables in your setup, please come and ask on the
`Daml forum <https://discuss.daml.com>`_ and we will be happy to help.

Open a terminal and run the following commands. Typos are a big problem here so
copy/paste one line at a time if possible. None of these should produce any
output on success. If you are running **bash**, run::

        echo 'export JAVA_HOME="$(/usr/libexec/java_home)"' >> ~/.bash_profile
        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.bash_profile

If you are running **zsh**, run::

        echo 'export JAVA_HOME="$(/usr/libexec/java_home)"' >> ~/.zprofile
        echo 'export PATH="$HOME/.daml/bin:$PATH"' >> ~/.zprofile

For both shells, the above will update the configuration for future, newly
opened terminals, but will not affect any exsting one. To test the
configuration of ``JAVA_HOME`` (on either shell), open a new terminal and run::

        echo $JAVA_HOME

You should see the path to the JDK installation, which is something like ``/Library/Java/JavaVirtualMachines/jdk_version_number/Contents/Home``.

Next, please verify the ``PATH`` variable by running (again, on either shell)::

        daml version

You should see a the header ``SDK versions:`` followed by a list of installed (or available) SDK versions (possibly a list of just one if you just installed).

If you do not see the expected outputs, please contact us on the `Daml forum <https://discuss.daml.com>`_ and we will be happy to help.


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
