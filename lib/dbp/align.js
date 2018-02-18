
const path = require('path');
const fs = require('fs');

const async = require('async');
const csv = require('csv-stream');
const pg = require('pg');

let ds_input = fs.createReadStream(path.join(__dirname, '../../data/input/dbpedia/Place.csv'));

let k_stream = csv.createStream({
	escapeChar: '"',
	enclosedChar: '"',
});


let y_client = new pg.Client(Object.assign(require(__dirname+'/../../postgres-config.json'), {
	database: 'usgs_dbp',
}));

let a_fields = ['URI', 'point', '22-rdf-syntax-ns#type', 'rdf-schema#label', 'isPartOf'];
let a_inserts = [];
let k_queue = async.queue((s_values, fk_done) => {
	let s_query = `insert into dbp (uri, geom, type, label, part_of) values ${s_values}`;
	y_client.query(s_query, (e_query, h_result) => {
		if(e_query) {
			debugger;
			// console.error(s_query);
			console.error(e_query);
		}
		else {
			console.log('+1');
		}

		fk_done();
	});
}, 1);

y_client.connect(function(e_connect) {
	if(e_connect) {
		throw new Error(`could not connect to database: `+e_connect);
	}

	ds_input.pipe(k_stream)
		.on('data', (h_row) => {
			let a_values = [];

			// skip those missing point
			if(!h_row.point) return;
			if('{' === h_row.point[0]) return;

			a_fields.forEach((s_field) => {
				let s_value = h_row[s_field];
				if(s_value === 'NULL') {
					a_values.push('null');
				}
				else if(s_field === 'point') {
					a_values.push(`'POINT(${s_value})'`);
				}
				else {
					a_values.push(`'${s_value.replace(/'/g, "''")}'`);
				}
			});

			a_inserts.push(`(${a_values.join(', ')})`);
			if(a_inserts.length >=20) {
				k_queue.push(a_inserts.join(','));
				a_inserts = [];
			}
		});
});
