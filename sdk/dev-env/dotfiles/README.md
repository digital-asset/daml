# Editor Daml Language Integration

## Vim

In order to enable vim syntax highlighting for our DSLs, execute the following
two lines in your terminal (assuming `~/dev` is where you've checked out digital-asset/daml)

```
mkdir -p ~/.vim/syntax
ln -s ~/dev/dotfiles/vim/syntax/*.vim ~/.vim/syntax/
```

You also want to make sure that you are sourcing the `.vimrc_da` file in
your `~/.vimrc`.

## VS Code

Follow the Daml Studio installation instructions in the Daml user guide.


## Update Keywords

The instructions on how to update the syntax highlighting of keywords in all our
supported editors is in `pkgs/da-hs-daml-base/src/DA/Daml/ReservedNames.hs`
(see documentation for `printEditorKeywords`).
## Stylish Haskell

Copy the `stylish-haskell.yaml` file either to `~/.stylish-haskell.yaml` or to
`~/dev/.stylish-haskell.yaml`. If you are using vim you can use the plugin
`jasperdvdj/stylish-haskell` to automatically style your code on write.
