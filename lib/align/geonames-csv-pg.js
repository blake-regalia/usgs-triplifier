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
	drop table if exists geonames;
	create table geonames (
		id text,
		name text,
		alternate_names text,
		geom geometry(Point, 4326)
	);
	create index idx_geonames_id on geonames(id);
	create index idx_geonames_name on geonames(name);
	create index gix_geonames_geom on geonames using gist(geom);
`;

(async function () {
	await s_sql_create.split(/;/g).map((s_sql) => async () => {
		await y_client.query(s_sql);
	}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());

	fs.createReadStream(process.argv[2])
		.pipe(csv_parser({
			delimiter: '\t',

			headers: [
				'id',
				'name',
				'ascii_name',
				'alternate_names',
				'lat',
				'lng',
				'class',
				'fcode',
				'ccode',
				'cc2',
				'adm1c',
				'adm2c',
				'adm3c',
				'population',
				'elevation',
				'dem',
				'timezone',
				'modified',
			],

			row(h_row) {
				a_inserts.push(`$${++i_ins}, $${++i_ins}, $${++i_ins}, ST_GeomFromEWKT('SRID=4326;POINT(${h_row.lng} ${h_row.lat})')`);
				a_values.push(...[
					h_row.id,
					h_row.name,
					h_row.alternate_names,
				]);
			},

			async progress() {
				let a_values_ref = a_values;
				let a_inserts_ref = a_inserts;
				a_values = [];
				a_inserts = [];
				i_ins = 0;

				let s_sql = /* syntax: sql */ `
					insert into geonames (id, name, alternate_names, geom)
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
