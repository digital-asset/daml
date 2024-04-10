# Daml Syntax Highlighter Dev Guide
In depth explanation syntax highlighting can be found on VS Code [website](https://code.visualstudio.com/api/language-extensions/syntax-highlight-guide). This document contains a short guide and explanations of choices made for Daml plugin.

## Overview
VS code uses [TextMate Grammar](https://macromates.com/manual/en/language_grammars). File processing is based on [Oniguruma regular expressions](https://macromates.com/manual/en/regular_expressions). These are generally written as JSON or [PList format](https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/PropertyLists/UnderstandXMLPlist/UnderstandXMLPlist.html).

## Grammar File Format.
VS Code supports PList, JSON and YAML files to specify grammar. Daml VS code extension makes use of PList, as the haskell [extension](https://github.com/JustusAdam/language-haskell) and also using Plist makes having comments easy at the expense of making it more verbose.


## Testing
Unfortunately at the moment we do not have a automated way to test. We have to depend on good old human eyes, to make that a little easy we have `TestGrammar.daml` in the same folder, a annotated file with expected scopes as comments. If there any changes to the scope please do update the comments.

Example: ` employee : Party -- employee -> source.daml , Party -> storage.type.daml > meta.type-declaration.daml > source.daml`

- ` -- ` Begin comment
- `employee ->` Token employee is scoped to `source.daml` this can be viewed via VS code scope inspector tool
- `Party ->` Token party has three nested scopes and in the scope inspector they are listed in order

## Daml Specific constructs

- Colon and double colon Daml considers `::` a cons operator and `:` as type operator.
- Keywords - there are a few daml specific keywords (eg: template, key, maintainer.).
- And context specific key words such as with, choice and controller.

The order of these daml specific constructs are important, especially the `:` and `::`. The same is true with choice as there can be more context aware elements, and there are two flavours of this one where choice is used and the other is where controller is used.


## Tools and References
- https://rubular.com/ - regex engine for quick feedback on regular expression.
- https://regex101.com/ -- regex engine, but has a lot of explanations and good for exploring.
- https://www.apeth.com/nonblog/stories/textmatebundle.html - TextMate advanced rules explained. Probably good to read before starting a major change.
