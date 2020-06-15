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
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Mac OS with ``bash`` console.
If yu are using ``zsh`` all of the instructions are quite similar, except that you will be doing all of the changes
in the ``.zprofile`` file.

Setting the JAVA_HOME variable
==============================
Open the ``~/.bash_profile`` in any text editor. The file should be located in your ``Home`` folder.
In ``Finder`` click on Go --> Home and then click ``Cmd+SHIFT+.`` to see the hidden files. Locate the ``~/.bash_profile``
and double click on it to open it. Add the folloiwing line as a new line at the end of the file::

        export JAVA_HOME=$(/usr/libexec/java_home)

Save the file before closing.

Next open the terminal and run the source command to apply the changes::

        source ~/.bash_profile

Check the ``JAVA_HOME`` variable and make sure that the changes have been applied::

        echo $JAVA_HOME

The result should be the path to the JDK installation, something like this::

        /Library/Java/JavaVirtualMachines/jdk_version_number/Contents/Home

Setting the PATH variable
=========================
Open the ``~/.bash_profile`` in any text editor. Add the following line as a new line at the
end of the file::

        export PATH="~/.daml/bin:$PATH"

Save the file before closing.

Next open the terminal and run the source command to apply the changes::

        source ~/.bash_profile

To check if DAML has been added to your ``PATH`` variable run::

        daml version

which should list out all installed DAML SDK version as well as the project default one. You should see a line stating::

        (default SDK version for new projects)

Linux
*****
We'll explain here how to set up ``JAVA_HOME`` and ``PATH`` variables on Linux for ``bash``terminal.

Setting the JAVA_HOME variable
==============================

Open ~/.bashrc in any text editor and add the path to your JDK. Typically it should be something like ``/usr/lib/jvm/java-version``::

        export JAVA_HOME=/usr/lib/jvm/java-version

Save and close the file.

Load the new settings by running::

        source ~/.bashrc

Check the value of the JAVA_HOME variable by running::

        echo $JAVA_HOME

The result should be the path to the JDK installation::

        /usr/lib/jvm/java-version

Setting the PATH variable
=========================

Open the ``~/.bash_profile`` in any text editor. Add the following line as a new line at the
end of the file::

        export PATH="~/.daml/bin:$PATH"

Save the file before closing.

Next open the terminal and run the source command to apply the changes::

        source ~/.bash_profile

To check if DAML has been added to your ``PATH`` variable run::

        daml version

which should list out all installed DAML SDK version as well as the project default one. You should see a line stating::

        (default SDK version for new projects)
