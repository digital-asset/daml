.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. This file track the change from daml-lf 1.dev


HEAD — ongoing
--------------

* Add Numerics

  + protobuf archive:
    - add `nat` kind 
    - add `nat` type
    - add `NUMERIC` primitive type
    - add `numeric` primitive literal
    - add numeric builtins, namely  `ADD_NUMERIC`, `SUB_NUMERIC`, `MUL_NUMERIC`, `DIV_NUMERIC`, `ROUND_NUMERIC`, `CAST_NUMERIC`, `SHIFT_NUMERIC`, `LEQ_NUMERIC`, `LESS_NUMERIC`, `GEQ_NUMERIC`, `GREATER_NUMERIC`, `FROM_TEXT_NUMERIC`, `TO_TEXT_NUMERIC`, `INT64_TO_NUMERIC`, `NUMERIC_TO_INT64`, `EQUAL_NUMERIC`
    - deprecate `DECIMAL` primitive type
    - deprecate `decimal` primitive literal   
    - deprecate decimal builtins, namely  `ADD_DECIMAL`, `SUB_DECIMAL`, `MUL_DECIMAL`, `DIV_DECIMAL`, `ROUND_DECIMAL`, `LEQ_DECIMAL`, `LESS_DECIMAL`, `GEQ_DECIMAL`, `GREATER_DECIMAL`, `FROM_TEXT_DECIMAL`, `TO_TEXT_DECIMAL`, `INT64_TO_DECIMAL`, `DECIMAL_TO_INT64`, `EQUAL_DECIMAL`

  + for more details refer to DAML-LF specification

* Add AnyTemplate

  + protobuf archive:
    - add `ANY` primitive type
    - add `ToAny` and `FromAny` expression

  + for more details refer to DAML-LF specification