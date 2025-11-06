.. _daml-packages-and-daml-archive-files:

Daml packages and archive (.dar) files
######################################

When a Daml package is compiled, it is packed into a final artifact called a DAR
(``.dar``) file. The purpose of this DAR file is to contain all of the necessary
code and logic to run the package's templates, without requiring any other
files.

For example, assume a simple package ``mypkg`` with a single dependency ``dep``:

.. code:: yaml

   name: mypkg
   version: 1.0.0
   source: daml
   ...
   dependencies:
   - daml-prim
   - daml-stdlib
   data-dependencies:
   - /path/to/dep-1.0.0.dar

The command ``dpm build`` compiles it and reports the path of the resulting DAR
as the last line:

.. code:: sh

   > dpm build

   Running single package build of mypkg as no multi-package.yaml was found.
   ...
   Compiling mypkg to a DAR.
   ...
   Created .daml/dist/mypkg-1.0.0.dar

This DAR will contain all of the code for the package, as well as all of the
code for its dependencies.

.. _structure-of-an-archive-file:

Structure of an archive file
****************************

A DAR is actually a zip file which contains many different files, all of which
work together to provide a ledger everything it needs to know in order to run
the code it was compiled from.

.. code:: sh

   > unzip -Z1 .daml/dist/mypkg-1.0.0.dar

   META-INF/MANIFEST.MF
   ...
   mypkg-1.0.0-<mypkg-package-id>/dep-1.0.0-<dep-package-id>.dalf
   mypkg-1.0.0-<mypkg-package-id>/Main.daml
   mypkg-1.0.0-<mypkg-package-id>/Main.hi
   mypkg-1.0.0-<mypkg-package-id>/Main.hie
   mypkg-1.0.0-<mypkg-package-id>/mypkg-1.0.0-<mypkg-package-id>.dalf


The majority of the files in a given DAR will be DALF files (``.dalf``). Each
``.dalf`` file contains the entire compiled code for a specific package.

One of the DALF files will be the "main" or "primary" package that the DAR was
compiled from - this DALF will contain the definitions of templates, interfaces,
datatypes, and functions that were originally described in the Daml code that
the DAR was compiled from. In this case, that is the ``mypkg-1.0.0-<mypkg-package-id>.dalf``
file listed above.

All of the other DALF files will be for dependency packages of that "main"
package, which are required to run the package. This includes the ``dep-1.0.0-<dep-package-id>.dalf``
file, as well as many DALF files for the ``daml-prim`` and ``daml-stdlib``
libraries.

Aside from these files, there will be:

* A ``MANIFEST.MF`` file, which contains metadata about the rest of the
  artifacts in the DAR.
  * the name of the "main" package that was compiled into the DAR
  * a list of all of the dependencies of the main package
  * some more metadata about the package
* The source code (``.daml``) for the primary package. This can be used by
  consumers of the DAR to verify that the DALF they're running corresponds to
  the code inside of it. It is also used by Daml Studio for code intelligence
  such as jump-to-definition when the DAR is included as a dependency of another
  project.
* Interface files (``.hi``, ``.hie``, ``.conf``) for the primary package. This
  is also used by Daml Studio to provide jump-to-definition.

Both the source code and interface files are not required by any other tool than
Daml Studio - they can be safely removed from the DAR by consumers such as
participant runners.

Difference between DALF files and Daml files
********************************************

A common question is why DAR files contain DALF files - why don't they just
contain all of the source code for all of the packages directly?

To understand why, it is important to understand the distinction between Daml
and DALF files:

* Daml files contain the Daml source code that Daml developers write. Daml
  source code is human-writable and human-readable, and is readable as a general
  purpose programming language.
* DALF files, on the other hand contain a compact, binary-encoded representation
  of Daml-LF. Daml-LF is very restricted, comparatively simple
  computer-executable programming language. Daml-LF is not intended to be
  human-readable nor human-writable, it is intended to be deterministic, fast to
  execute, and secure.

Because DAR files are intended to be executed and passed around, they primarily
contain Daml-LF, which can be executed directly -- they do not need to store the
Daml code from which it was compiled.

DARs as dependencies
********************

When a new project needs to depend on a different package, the DAR that the
package was compiled to is supplied as a data-dependency in the new project's
``daml.yaml``.

For example, suppose a new package ``next-project`` that uses the ``mypkg``
package as a dependency:

.. code:: yaml

   name: next-project
   version: 1.0.0
   source: daml
   dependencies:
   - daml-prim
   - daml-stdlib
   data-dependencies:
   - ../mypkg/.daml/dist/mypkg-1.0.0.dar

In this case, the compilation process unpacks the ``mypkg-1.0.0`` DAR, finds its
primary package, and exposes that as a dependency to code inside
``next-project``. When ``next-project`` is compiled, it retains all of the DALF
files inside the ``mypkg`` DAR, including the ``mypkg`` package's dependencies.

In general, any time a DAR is compiled for a package that has further DAR
dependencies, those DAR dependencies are unpacked and all of their DALF files
are copied into the new output DAR. However, while DALF files are copied over,
the dependency DARs' manifest files are not copied over, and neither are the
source code and interface files. Only the source code and interface files for
the primary package of a DAR can show up in a DAR.

For more information on how to open up and inspect the DAR files and DALF files,
refer to the documentation on :ref:`how to parse Daml archive files
<how-to-parse-daml-archive-files>`.
