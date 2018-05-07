const fs = require('fs');
const pg = require('pg');
const csv_parser = require('../util/csv-parser.js');

let c_rows = 0;
let i_ins = 0;
let a_inserts = [];
let a_values = [];

let y_client = new pg.Client();
y_client.connect();

let s_sql_create = /* syntax: sql */ `
	drop table if exists gnis;
	create table gnis (
		id text,
		name text,
		alternate_names text,
		geom geometry(Point, 4326)
	);
	create index gix_gnis_geom on gnis using gist(geom);
	create index idx_gnis_name on gnis(name);
`;

(async function () {
	await s_sql_create.split(/;/g).map((s_sql) => async () => {
		await y_client.query(s_sql);
	}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());

	fs.createReadStream(process.argv[2])
		.pipe(csv_parser({
			delimiter: '|',

			row(h_row) {
				a_inserts.push(`$${++i_ins}, $${++i_ins}, NULL, ST_GeomFromEWKT('SRID=4326;POINT(${h_row.prim_long_dec} ${h_row.prim_lat_dec})')`);
				a_values.push(...[
					h_row.feature_id.replace(/^0+/, '') || '0',
					h_row.feature_name.replace(/\s\(historical\)$/, ''),
				]);
			},

			async progress() {
				let a_values_ref = a_values;
				let a_inserts_ref = a_inserts;
				a_values = [];
				a_inserts = [];
				i_ins = 0;

				let s_sql = /* syntax: sql */ `
					insert into gnis (id, name, alternate_names, geom)
					values ${a_inserts_ref.map(s => `(${s})`).join(', ')}
				`;

				let h_res = await y_client.query(s_sql, a_values_ref);
				console.log(`${h_res.rowCount} rows inserted`);
			},

			end() {
				console.log(`rows: ${c_rows}`);
			},
		}));
})();
