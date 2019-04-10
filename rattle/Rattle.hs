
module Rattle(rattle, CmdOption(..), cmd, cmd_, Stdout(..), Exit(..), Stderr(..)) where

import Development.Shake.Command

rattle :: IO () -> IO ()
rattle act = act
