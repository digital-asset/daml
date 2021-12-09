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

## How the docs get built

The docs get built as part of the main `daml` repo CI, to make sure we don't break any links or do anything else that would cause Sphinx warnings.

## Publishing docs

Documentation is published automatically whenever a release is made
public on Github. Note that there is a delay so you might have to wait
up to an hour until the docs are published after making a release
public.

## Testing code in docs

TBD
