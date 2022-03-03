# Daml Documentation

This directory contains all of the documentation that gets published to docs.daml.com.

## Writing documentation

The docs are written in [reStructuredText](http://docutils.sourceforge.net/rst.html), built using [Sphinx](http://www.sphinx-doc.org/en/master/).

To edit documentation:

- Same as code: find the file, edit it on a branch, make a PR.
- For new files, best to copy existing ones, to get the formatting right. 

  Don't forget you need to add your file to the `toctree` in `/docs/source/index.rst` *and* `/docs/configs/pdf/index.rst`.
- **Make sure you preview** before you push.
- Don't insert line-breaks inside inline literals. Building preview will treat this as an error.

### Generated documentation

Not all of our docs are in rst files: some get generated. They are:

- the ledger API proto docs,
- the Daml standard library reference,
- the Java bindings reference,
- error codes inventory.

To edit those docs, edit the content inside the code source.

### Previewing

To preview the full docs, as deployed to docs.daml.com, run `scripts/preview.sh`.

To live-preview the docs, run `scripts/live-preview.sh`. The script accepts two flags:

- `--pdf` includes the PDF documentation,
- `--gen` includes the generated documentation.

Note that neither PDF, nor generated docs will benefit from live updates. 
To update generated docs or PDF docs, quit the preview script with CTRL+C and start it again.

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
- For bullet points (unordered lists), use `-` (dashes).
- For code blocks, use the `literalinclude` directive if you can: it's best to source code from files that we test whether they compile.

## Updating the table of contents

The table of contents is generated automatically based on the titles
and `:toctree:` entries in the Rst files. However, the root index
files `docs/source/index.rst` and `docs/configs/pdf/index.rst` are
special in two ways:

First, there are different versions for the HTML and the PDF
docs build. These should be kept in sync with the difference beeing
that in the HTML guide we hide we use captions on `:toctree:` entries
and mark them as `:hidden:` because the ToC shows up in the sidebar
while in the PDF version we use section headers instead of captions
and do not hide the `:toctree:` entries.

The second the `index.rst` files in the Daml repository are only used
 for preview in the Daml repository and have no effect on the the
 published documentation. Instead, we replace them by the
 [`index_html.rst`](https://github.com/DACH-NY/assembly/blob/main/docs/index/index_html.rst)
 and
 [`index_pdf.rst`](https://github.com/DACH-NY/assembly/blob/main/docs/index/index_pdf.rst)
 files in the assembly repo which combine the documentation from the
 Daml and the Canton repository. So if you change the root index files
 in the Daml repository make sure that you apply the same change in
 the assembly repository.

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
