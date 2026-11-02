// Cube configuration: how Cube reaches the lake. DuckDB attaches the DuckLake catalog READ_ONLY as lake_ro
// (compose gives this container lake_ro's password and only that), so nothing queried through Cube can change
// the lake. The attach applies to the whole DuckDB instance, so models name tables in full (lake.gold.<table>)
// rather than relying on USE, which would only reach the first connection.
//
// The driver logs and skips a failing initSql instead of stopping, so acceptance/test_cube.py checks real answers
// through the SQL API, not just that Cube started.
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const fs = require('fs');
const path = require('path');

const ATTACH_LAKE = [
  'INSTALL ducklake', 'LOAD ducklake', 'INSTALL postgres', 'LOAD postgres',
  "ATTACH 'ducklake:postgres:dbname=ducklake host=catalog-pg user=lake_ro' AS lake "
    + "(DATA_PATH '/lake/data/', READ_ONLY)",
].join(';\n');

/** True while course 1's add-on (lakehouse/course1_addon.py) is on: LAKEHOUSE_COURSE1 unset or "on"; false for
 *  "off"; anything else stops Cube, so a typo can't quietly drop the add-on. */
function course1Enabled() {
  const value = (process.env.LAKEHOUSE_COURSE1 || 'on').trim().toLowerCase();
  if (value !== 'on' && value !== 'off') throw new Error(`LAKEHOUSE_COURSE1 must be on or off, not ${value}`);
  return value === 'on';
}

/** The model files Cube compiles: everything in model/, plus course1/ (the Trust Test cube) while course 1's
 *  add-on is on. */
function modelFiles() {
  const folders = ['model', ...(course1Enabled() ? ['course1'] : [])];
  return folders.flatMap((folder) => fs.readdirSync(path.join(__dirname, folder))
    .filter((name) => /\.(ya?ml|js)$/.test(name)).sort()
    .map((name) => ({ fileName: `${folder}/${name}`,
                      content: fs.readFileSync(path.join(__dirname, folder, name), 'utf8') })));
}

module.exports = {
  /** Build the DuckDB driver Cube uses for every query, with the lake attached read-only. */
  driverFactory: () => new DuckDBDriver({ initSql: ATTACH_LAKE }),
  /** Cube groups for a request: a REST token's `groups` claim, else `default`. Access policies in the model
   *  mask restricted columns for every group except restricted_reader. */
  /** Where the model comes from: modelFiles(), so course 1's add-on follows LAKEHOUSE_COURSE1. */
  repositoryFactory: () => ({ dataSchemaFiles: async () => modelFiles() }),
  contextToGroups: ({ securityContext }) => (securityContext && securityContext.groups) || ['default'],
};
