#!/bin/bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


cd scripts


STAINLESS=$1
ARGS="--watch=false --timeout=30 --vc-cache=false --compact=true --solvers=nativez3 --infer-measures=false"

#Running stainless, there are 3 modes:
# - translate: translate the original file to a simplified version and verifies only that
#              the translation is correct
# - verification: verifies the proof
# - test: test that stainless is executed correctly; does not verify any file
# Finally returns the exit code of stainless:
# 0 : everything verifies
# 1 : something does not verify
# 2 : the files do not compile

if [[ $2 = "translate" ]]; then
  DAML_ROOT=$(git rev-parse --show-toplevel);
  FILE_LOCATION="$DAML_ROOT/daml-lf/transaction/src/main/scala/com/digitalasset/daml/lf/transaction/ContractStateMachine.scala";

  #We first load the original file in a variable
  FILE=$(cat $FILE_LOCATION)

  #To be able to quickly double check the output we first remove comments and emptyLines
  FILE=$(sed '/^\s*\/\*/d' <<<"$FILE");
  FILE=$(sed '/^\s*\*/d' <<<"$FILE");
  FILE=$(sed  '/^\s*\/\//d' <<<"$FILE");
  FILE=$(sed  '/^$/d' <<<"$FILE");

  #Replacing covariant options and lists by invariant ones
  FILE=$(sed 's/\bNone\b/None()/g' <<<"$FILE");
  FILE=$(sed 's/\([A-Za-z0-9_]*\)\s*::\s*\([A-Za-z0-9_]*\)/Cons(\1, \2)/g' <<<"$FILE");
  FILE=$(sed 's/\(\([A-Za-z0-9_]\|\.\)*\).filterNot(\(\([A-Za-z0-9_]\|\.\)*\))/Option.filterNot(\1, \3)/g' <<<"$FILE");
  FILE=$(sed 's/\bNil\b/Nil()/g' <<<"$FILE");

  #Replacing KeyMapping with Option
  FILE=$(sed '/^\s*val\s*KeyActive\s*=/d' <<<"$FILE");
  FILE=$(sed '/^\s*val\s*KeyInactive\s*=/d' <<<"$FILE");
  FILE=$(sed 's/\s\(ContractStateMachine\.\)\?KeyInactive\b/ None[ContractId]()/g' <<<"$FILE");
  FILE=$(sed 's/\s\(ContractStateMachine\.\)\?KeyActive\b/ Some[ContractId]/g' <<<"$FILE");
  FILE=$(sed '/^\s*val KeyActive/d' <<<"$FILE");

  #Replacing exceptions with Unreachable
  FILE=$(sed -z 's/throw\s*new\s*[A-Za-z0-9]\+(\s*\("\([A-Za-z0-9(),;:_]\|\s\|\[\|\]\|\-\|\.\|\/\)*"\)\?\s*)/Unreachable()/g' <<<"$FILE");
  FILE=$(sed 's/throw\s*new\s*[A-Za-z0-9]\+(\("\([A-Za-z0-9(),;:_]\|\s\|\[\|\]\|\-\|\.\|\/\)*"\)\?)/Unreachable()/' <<<"$FILE");


  #Replacing imports and package name by our own (located in $2) and writing the output in translation/ContractStateMachine.scala
  FILE=$(sed '/^package/d' <<<"$FILE");
  FILE=$(sed -z 's/\s*import\s*\([A-Za-z0-9]\|\.\)\+{\([A-Za-z0-9(),:;]\|\s\|\[\|\]\|\-\|\.\|\/\)*}//g' <<<"$FILE");
  FILE=$(sed '/^\s*import\s*\([A-Za-z0-9]\|\.\)\+/d' <<<"$FILE");
  FILE=$(sed 's/\(\s*\)\(case class State\)/\1@dropVCs\n\1\2/' <<<"$FILE");
  
  ADD=$(cat stainless_imports.txt);
  FILE_DESTINATION="../translation/ContractStateMachine.scala"
  echo -e "${ADD}$FILE" > $FILE_DESTINATION;

  $STAINLESS ../utils/* ../translation/* ../transaction/* $ARGS;
  
  RES=$?

  #Cleaning everything up
  rm $FILE_DESTINATION
  
  exit $RES

elif [[ $2 = "verify" ]]; then
  $STAINLESS ../utils/* ../transaction/* ../tree/* $ARGS;
  exit $?

elif [[ $2 = "test" ]]; then
  $STAINLESS $ARGS;
  exit $?
else
  exit 3
fi




