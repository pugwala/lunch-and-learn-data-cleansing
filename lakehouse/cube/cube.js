// Cube configuration: how Cube reaches the lake. DuckDB attaches the DuckLake catalog READ_ONLY as lake_ro
// (compose gives this container lake_ro's password and only that), so nothing queried through Cube can change
// the lake. The attach applies to the whole DuckDB instance, so models name tables in full (lake.gold.<table>)
// rather than relying on USE, which would only reach the first connection.
//
// The driver logs and skips a failing initSql instead of stopping, so acceptance/test_cube.py checks real answers
// through the SQL API, not just that Cube started.
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');

const ATTACH_LAKE = [
  'INSTALL ducklake', 'LOAD ducklake', 'INSTALL postgres', 'LOAD postgres',
  "ATTACH 'ducklake:postgres:dbname=ducklake host=catalog-pg user=lake_ro' AS lake "
    + "(DATA_PATH '/lake/data/', READ_ONLY)",
].join(';\n');

module.exports = {
  /** Build the DuckDB driver Cube uses for every query, with the lake attached read-only. */
  driverFactory: () => new DuckDBDriver({ initSql: ATTACH_LAKE }),
  /** Cube groups for a request: a REST token's `groups` claim, else `default`. Access policies in the model
   *  mask restricted columns for every group except restricted_reader. */
  contextToGroups: ({ securityContext }) => (securityContext && securityContext.groups) || ['default'],
};
