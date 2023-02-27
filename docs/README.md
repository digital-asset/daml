# Daml Documentation

This directory contains all of the documentation that gets published to docs.daml.com.

## Writing documentation

The docs are written in [reStructuredText](http://docutils.sourceforge.net/rst.html), built using [Sphinx](http://www.sphinx-doc.org/en/master/).

To edit documentation:

- Same as code: find the file, edit it on a branch, make a PR.
- For new files, best to copy existing ones, to get the formatting right. 
- If you need to change the table of contents, look at the section about it further down
- **Make sure you preview** before you push. Maybe include screenshots in a comment for your PR if relevant.
- Don't insert line-breaks inside inline literals. Building preview will treat this as an error.

### Generated documentation

Not all of our docs are in rst files: some get generated. They are:

- the Ledger API proto docs,
- the Daml standard library reference,
- the Java bindings reference,
- error codes inventory.

To edit those docs, edit the content inside the code source.

### Previewing

To preview the docs run `scripts/preview.sh`.

To live-preview the docs, run `scripts/live-preview.sh`. The script accepts two flags:

- `--pdf` includes the PDF documentation,
- `--gen` includes the generated documentation.

Note that neither PDF, nor generated docs will benefit from live updates. 
To update generated docs or PDF docs, quit the preview script with CTRL+C and start it again.

The preview is indicative of contents documented in this repository. For the final result, you
should go to https://github.com/digital-asset/docs.daml.com and consult the preview instructions.

### Style conventions

For terminology and other style questions, follow the [main DA documentation style guide](https://docs.google.com/document/d/1dwE45gyxWXqlr4VTq9mJVnmSyBQ8V30ItucWBbCbViQ/edit).

A few pieces of RST guidance:

If you’re not familiar, it’s really worth reading the [primer](http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html) for the basic syntax (emphasis, code text, lists, tables, images, comments, etc).
- Keep paragraphs all on the same line (no newlines/line breaks).
- Heading underlines in this hierarchical order:

  ```
  ######
  ******
  ======
  ------
  ^^^^^^
  """"""
  ```
- For internal links, use the `doc` directive where you can. 
- Before any lists in an .rst file, be sure to add an extra blank line. There should be two carriage returns between the text that comes before a list and the list itself. Otherwise, the list items will all be merged into a single paragraph.
- For bullet points (unordered lists), use `-` (dashes).
- For code blocks, use the `literalinclude` directive if you can: it's best to source code from files that we test whether they compile.

## Updating the table of contents

The table of contents is handled in https://github.com/digital-asset/docs.daml.com.

If you want need to add an item that needs to end up on the table of contents, you need to:

1. get the PR that introduces the new item
2. wait for the following daily snapshot
3. follow the instructions on https://github.com/digital-asset/docs.daml.com, in particular with regards to
   [making changes to the next unreleased version](https://github.com/digital-asset/docs.daml.com#making-changes-to-the-next-unreleased-version)

## Linking to pages from the Canton documentation

This repository only includes content for the Daml SDK. However the Daml documentation includes both
content from this repository and from [Canton's](https://github.com/digital-asset/canton). This means that
linking to a page in the Canton documentation requires some additional care. In particular, you have to:

1. find the anchor point to which you want to link in the Canton documentation
2. add the anchor point name in the `//docs/canton-refs.rst` document (unless it's already there)
3. refer to the anchor point in your page

You can see a small example of this procedure in
[this commit](https://github.com/digital-asset/daml/commit/30acaaea777b92712fe46c2062361f5f44b260ce).

## How the docs get built

The final documentation gets built as part of the `assembly`
repository but the `daml` repository builds the Daml part of the
documentation and checks that the sphinx builds passes without
warnings.

## Publishing docs

Documentation is published automatically whenever a release is made
under `https://docs.daml.com/$version/`.  The documentation at
`https://docs.daml.com` is updated by an hourly cron job to the latest
version that has been marked as a non-prerelease on Github.

## Testing code in docs

TBD

## Building the assembly repo against HEAD

Especially for changes to the table of contents, it can often be
useful to see the final version of the docs that is built by the
assembly repo based on the current HEAD of the daml repository. For
that, you can build and copy the artifacts from the Daml repository to
the `download` directory of the assembly repository as follows:


```
./docs/scripts/copy-assembly path/to/assembly/docs/download
```

Afterwards, you can use the usual commands from the [assembly
repository](https://github.com/DACH-NY/assembly/blob/main/docs/README.md)
to build documentation where you use `0.0.0` as the SDK version, e.g.,

```
./live-preview.sh 0.0.0 2.0.0-rc9
```
