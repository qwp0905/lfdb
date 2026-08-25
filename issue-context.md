# README Configuration table doesn't list default values for EngineBuilder options

The `Configuration` table in the README documents what each `EngineBuilder` option does, but never states its default value:

```
| `wal_file_size` | Size limit of a single WAL segment file. ... |
| `block_cache_memory_capacity` | Total memory allocated to the block cache. ... |
| `gc_thread_count` | Number of GC threads. ... |
...
```

Every one of these has a real default (`DEFAULT_WAL_FILE_SIZE`, `DEFAULT_BLOCK_CACHE_MEMORY_CAPACITY`, `DEFAULT_GC_THREAD_COUNT`, etc. in `src/builder.rs`), but a user reading the README has no way to tell:
- which knobs are already reasonable and safe to leave alone, vs.
- which ones they should actively set for their workload,

without going and reading `EngineConfig`'s construction in `builder.rs` directly. That's especially awkward for capacity-ish settings like `block_cache_memory_capacity` or `wal_file_size`, where "what do I get if I don't call this builder method" is exactly the first question a new user has.

**Suggestion**: add a `Default` column to the Configuration table (or inline the default value into each row's description), sourced from the `DEFAULT_*` constants in `src/builder.rs`, so the table is self-contained.


https://github.com/qwp0905/lfdb/issues/293

## Recent issue comments

### qwp0905 at 2026-08-14T13:18:50Z
@ppijbb I think this is an excellent suggestion for improving usability. It’s a great idea—I had simply been putting it off because it seemed like a hassle.

### ppijbb at 2026-08-25T14:08:08Z
Nightwelding could not complete this issue overnight.

Reason: Reproduction diff touched no files.

Re-add the `nightwelding-queue` label to retry after addressing the root cause.
