# AnyBlox

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension integrates AnyBlox into DuckDB and allows reading of `.any` files directly with the `anyblox` functions, e.g.

```sql
SELECT * FROM anyblox('dataset-path.any');
```

## Building

First you need to build and install `libanyblox`, as in `anyblox-cpplib` in the main AnyBlox repo.

To build the extension execute `GEN=ninja make debug` (or `GEN=ninja make release` for the release configuration).

## Running

After building run the `duckdb` executable. The extension will be automatically loaded and ready to use.

Some usage examples can be found in the `scripts` directory, these are not cleaned up yet and e.g. hardcode paths for our specific machine.
