-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Debug.Trace.Extended (
  module Debug.Trace,
  tracePrettyId,
  traceMPretty
) where

import Debug.Trace
import DA.Pretty

--
-- | Like 'traceShowId' but pretty prints instead
--
tracePrettyId :: Pretty a => a -> a
tracePrettyId a = trace (renderPretty a) a

-- | Trace data with a @Pretty@ instance, in an applicative context,
-- and prefix the message with @prefix@.
traceMPretty :: (Pretty a, Applicative f) => String -> a -> f ()
traceMPretty prefix = traceM . (prefix++) . renderPretty
