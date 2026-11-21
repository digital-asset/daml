.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-text-83238:

DA.Text
=======

Functions for working with Text\.

Functions
---------

.. _function-da-text-explode-24206:

`explode <function-da-text-explode-24206_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\]

.. _function-da-text-implode-82253:

`implode <function-da-text-implode-82253_>`_
  \: \[:ref:`Text <type-ghc-types-text-51952>`\] \-\> :ref:`Text <type-ghc-types-text-51952>`

.. _function-da-text-isempty-39554:

`isEmpty <function-da-text-isempty-39554_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Test for emptiness\.

.. _function-da-text-isnotempty-43984:

`isNotEmpty <function-da-text-isnotempty-43984_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Test for non\-emptiness\.

.. _function-da-text-length-94326:

`length <function-da-text-length-94326_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Int <type-ghc-types-int-37261>`

  Compute the number of symbols in the text\.

.. _function-da-text-trim-11808:

`trim <function-da-text-trim-11808_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Remove spaces from either side of the given text\.

.. _function-da-text-replace-9445:

`replace <function-da-text-replace-9445_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Replace a subsequence everywhere it occurs\. The first argument
  must not be empty\.

.. _function-da-text-lines-25154:

`lines <function-da-text-lines-25154_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\]

  Breaks a ``Text`` value up into a list of ``Text``'s at newline
  symbols\. The resulting texts do not contain newline symbols\.

.. _function-da-text-unlines-66467:

`unlines <function-da-text-unlines-66467_>`_
  \: \[:ref:`Text <type-ghc-types-text-51952>`\] \-\> :ref:`Text <type-ghc-types-text-51952>`

  Joins lines, after appending a terminating newline to each\.

.. _function-da-text-words-34636:

`words <function-da-text-words-34636_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\]

  Breaks a 'Text' up into a list of words, delimited by symbols
  representing white space\.

.. _function-da-text-unwords-40113:

`unwords <function-da-text-unwords-40113_>`_
  \: \[:ref:`Text <type-ghc-types-text-51952>`\] \-\> :ref:`Text <type-ghc-types-text-51952>`

  Joins words using single space symbols\.

.. _function-da-text-linesby-11211:

`linesBy <function-da-text-linesby-11211_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\]

  A variant of ``lines`` with a custom test\. In particular, if there
  is a trailing separator it will be discarded\.

.. _function-da-text-wordsby-15461:

`wordsBy <function-da-text-wordsby-15461_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\]

  A variant of ``words`` with a custom test\. In particular, adjacent
  separators are discarded, as are leading or trailing separators\.

.. _function-da-text-intercalate-63059:

`intercalate <function-da-text-intercalate-63059_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\] \-\> :ref:`Text <type-ghc-types-text-51952>`

  ``intercalate`` inserts the text argument ``t`` in between the items
  in ``ts`` and concatenates the result\.

.. _function-da-text-dropprefix-62361:

`dropPrefix <function-da-text-dropprefix-62361_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  ``dropPrefix`` drops the given prefix from the argument\. It returns
  the original text if the text doesn't start with the given prefix\.

.. _function-da-text-dropsuffix-37682:

`dropSuffix <function-da-text-dropsuffix-37682_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Drops the given suffix from the argument\. It returns the original
  text if the text doesn't end with the given suffix\. Examples\:

  .. code-block:: daml

      dropSuffix "!" "Hello World!"  == "Hello World"
      dropSuffix "!" "Hello World!!" == "Hello World!"
      dropSuffix "!" "Hello World."  == "Hello World."

.. _function-da-text-stripsuffix-58624:

`stripSuffix <function-da-text-stripsuffix-58624_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Text <type-ghc-types-text-51952>`

  Return the prefix of the second text if its suffix matches the
  entire first text\. Examples\:

  .. code-block:: daml

      stripSuffix "bar" "foobar" == Some "foo"
      stripSuffix ""    "baz"    == Some "baz"
      stripSuffix "foo" "quux"   == None

.. _function-da-text-stripprefix-74987:

`stripPrefix <function-da-text-stripprefix-74987_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Text <type-ghc-types-text-51952>`

  The ``stripPrefix`` function drops the given prefix from the
  argument text\. It returns ``None`` if the text did not start with
  the prefix\.

.. _function-da-text-isprefixof-82357:

`isPrefixOf <function-da-text-isprefixof-82357_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isPrefixOf`` function takes two text arguments and returns
  ``True`` if and only if the first is a prefix of the second\.

.. _function-da-text-issuffixof-35218:

`isSuffixOf <function-da-text-issuffixof-35218_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isSuffixOf`` function takes two text arguments and returns
  ``True`` if and only if the first is a suffix of the second\.

.. _function-da-text-isinfixof-98358:

`isInfixOf <function-da-text-isinfixof-98358_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isInfixOf`` function takes two text arguments and returns
  ``True`` if and only if the first is contained, wholly and intact,
  anywhere within the second\.

.. _function-da-text-takewhile-40431:

`takeWhile <function-da-text-takewhile-40431_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  The function ``takeWhile``, applied to a predicate ``p`` and a text,
  returns the longest prefix (possibly empty) of symbols that satisfy
  ``p``\.

.. _function-da-text-takewhileend-32455:

`takeWhileEnd <function-da-text-takewhileend-32455_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  The function 'takeWhileEnd', applied to a predicate ``p`` and a
  'Text', returns the longest suffix (possibly empty) of elements
  that satisfy ``p``\.

.. _function-da-text-dropwhile-46373:

`dropWhile <function-da-text-dropwhile-46373_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  ``dropWhile p t`` returns the suffix remaining after ``takeWhile p t``\.

.. _function-da-text-dropwhileend-2917:

`dropWhileEnd <function-da-text-dropwhileend-2917_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  ``dropWhileEnd p t`` returns the prefix remaining after dropping
  symbols that satisfy the predicate ``p`` from the end of ``t``\.

.. _function-da-text-spliton-44082:

`splitOn <function-da-text-spliton-44082_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Text <type-ghc-types-text-51952>`\]

  Break a text into pieces separated by the first text argument
  (which cannot be empty), consuming the delimiter\.

.. _function-da-text-splitat-25614:

`splitAt <function-da-text-splitat-25614_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> (:ref:`Text <type-ghc-types-text-51952>`, :ref:`Text <type-ghc-types-text-51952>`)

  Split a text before a given position so that for ``0 <= n <= length t``,
  ``length (fst (splitAt n t)) == n``\.

.. _function-da-text-take-27133:

`take <function-da-text-take-27133_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  ``take n``, applied to a text ``t``, returns the prefix of ``t`` of
  length ``n``, or ``t`` itself if ``n`` is greater than the length of ``t``\.

.. _function-da-text-drop-34163:

`drop <function-da-text-drop-34163_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  ``drop n``, applied to a text ``t``, returns the suffix of ``t`` after
  the first ``n`` characters, or the empty ``Text`` if ``n`` is greater
  than the length of ``t``\.

.. _function-da-text-substring-36270:

`substring <function-da-text-substring-36270_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Compute the sequence of symbols of length ``l`` in the argument
  text starting at ``s``\.

.. _function-da-text-ispred-73747:

`isPred <function-da-text-ispred-73747_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isPred f t`` returns ``True`` if ``t`` is not empty and ``f`` is ``True``
  for all symbols in ``t``\.

.. _function-da-text-isspace-72803:

`isSpace <function-da-text-isspace-72803_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isSpace t`` is ``True`` if ``t`` is not empty and consists only of
  spaces\.

.. _function-da-text-isnewline-85831:

`isNewLine <function-da-text-isnewline-85831_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isSpace t`` is ``True`` if ``t`` is not empty and consists only of
  newlines\.

.. _function-da-text-isupper-58977:

`isUpper <function-da-text-isupper-58977_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isUpper t`` is ``True`` if ``t`` is not empty and consists only of
  uppercase symbols\.

.. _function-da-text-islower-60966:

`isLower <function-da-text-islower-60966_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isLower t`` is ``True`` if ``t`` is not empty and consists only of
  lowercase symbols\.

.. _function-da-text-isdigit-15622:

`isDigit <function-da-text-isdigit-15622_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isDigit t`` is ``True`` if ``t`` is not empty and consists only of
  digit symbols\.

.. _function-da-text-isalpha-72233:

`isAlpha <function-da-text-isalpha-72233_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isAlpha t`` is ``True`` if ``t`` is not empty and consists only of
  alphabet symbols\.

.. _function-da-text-isalphanum-87978:

`isAlphaNum <function-da-text-isalphanum-87978_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isAlphaNum t`` is ``True`` if ``t`` is not empty and consists only of
  alphanumeric symbols\.

.. _function-da-text-parseint-736:

`parseInt <function-da-text-parseint-736_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Int <type-ghc-types-int-37261>`

  Attempt to parse an ``Int`` value from a given ``Text``\.

.. _function-da-text-parsenumeric-9858:

`parseNumeric <function-da-text-parsenumeric-9858_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n \=\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` (:ref:`Numeric <type-ghc-types-numeric-891>` n)

  Attempt to parse a ``Numeric`` value from a given ``Text``\.
  To get ``Some`` value, the text must follow the regex
  ``(-|\+)?[0-9]+(\.[0-9]+)?``
  In particular, the shorthands ``".12"`` and ``"12."`` do not work,
  but the value can be prefixed with ``+``\.
  Leading and trailing zeros are fine, however spaces are not\.
  Examples\:

  .. code-block:: daml

      parseNumeric "3.14" == Some 3.14
      parseNumeric "+12.0" == Some 12

.. _function-da-text-parsedecimal-57278:

`parseDecimal <function-da-text-parsedecimal-57278_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Decimal <type-ghc-types-decimal-18135>`

  Attempt to parse a ``Decimal`` value from a given ``Text``\.
  To get ``Some`` value, the text must follow the regex
  ``(-|\+)?[0-9]+(\.[0-9]+)?``
  In particular, the shorthands ``".12"`` and ``"12."`` do not work,
  but the value can be prefixed with ``+``\.
  Leading and trailing zeros are fine, however spaces are not\.
  Examples\:

  .. code-block:: daml

      parseDecimal "3.14" == Some 3.14
      parseDecimal "+12.0" == Some 12

.. _function-da-text-sha256-29291:

`sha256 <function-da-text-sha256-29291_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Computes the SHA256 hash of the UTF8 bytes of the ``Text``, and returns it in its hex\-encoded
  form\. The hex encoding uses lowercase letters\.

  This function will crash at runtime if you compile Daml to Daml\-LF \< 1\.2\.

.. _function-da-text-reverse-37387:

`reverse <function-da-text-reverse-37387_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Reverse some ``Text``\.

  .. code-block:: daml

      reverse "Daml" == "lmaD"

.. _function-da-text-tocodepoints-44801:

`toCodePoints <function-da-text-tocodepoints-44801_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> \[:ref:`Int <type-ghc-types-int-37261>`\]

  Convert a ``Text`` into a sequence of unicode code points\.

.. _function-da-text-fromcodepoints-94464:

`fromCodePoints <function-da-text-fromcodepoints-94464_>`_
  \: \[:ref:`Int <type-ghc-types-int-37261>`\] \-\> :ref:`Text <type-ghc-types-text-51952>`

  Convert a sequence of unicode code points into a ``Text``\. Raises an
  exception if any of the code points is invalid\.

.. _function-da-text-asciitolower-24557:

`asciiToLower <function-da-text-asciitolower-24557_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Convert the uppercase ASCII characters of a ``Text`` to lowercase;
  all other characters remain unchanged\.

.. _function-da-text-asciitoupper-96826:

`asciiToUpper <function-da-text-asciitoupper-96826_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>`

  Convert the lowercase ASCII characters of a ``Text`` to uppercase;
  all other characters remain unchanged\.
