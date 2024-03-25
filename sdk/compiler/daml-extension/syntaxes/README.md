# README

Some notes on the files you find here.

  - `daml.json` defines the syntax highlighting rules used before Daml
    1.2 (`daml.configuration.json` in the parent directory
    configuration properties);
  - `daml12.tmLanguage.xml` defines the syntax highlight rules used
    starting with Daml 1.2;
    - These rules are in fact general Haskell syntax highlighting
      rules augmented with Daml specific keywords;
    - `daml12.tmLanguage.xml` was produced from the file
      `daml12.tmLanguage` by means of the VS-code extension,
      ["TextMate
      Languages"](https://marketplace.visualstudio.com/items?itemName=Togusa09.tmlanguage);
    - `daml12.tmLanguage.jsoxmln` and `daml12.configuration.json` (in the
      parent directory) are therefore (legal) derivative works subject
      to license conditions the details of which are provided in the
      file `daml12.tmLanguage.license.txt`.
