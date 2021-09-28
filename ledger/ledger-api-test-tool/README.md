## Suites

### Command deduplication suites

* default - is it a default test suite which does not need to be included explicitly
* append-only - Requires the schema to be append-only because we use the submission id set in the completion, which is
  present only for append-only schemas
* configuration-required - If it requires specific settings to be set for the ledger configuration

| Name | Default | Append only | Configuration required | Details |
| --- | --- | --- | --- | --- |
|CommandDeduplicationIT|Yes|No| No  |Tests participant deduplication|
|KVCommandDeduplicationIT| No | No | minSkew set to 1 second. maxDeduplicationDuration has to be < 5s | Extends the test cases from `CommandDeduplicationIT` with committer side test cases. Requires the time model update because KV committer deduplication is based on maxDeduplicationDuration + minSkew|
|AppendOnlyKVCommandDeduplicationIT|No|Yes|Same as KVCommandDeduplicationIT | Same as `KVCommandDeduplicationIT` but it requires an append-only schema so that we have access to the submission id|
|AppendOnlyCommandDeduplicationParallelIT | No |Yes| trackerRetentionPeriod has to be set to <= 5s | Requires append only schema so that we have access to the submission id |