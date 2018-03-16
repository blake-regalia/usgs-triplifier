
const path = require('path');
const fs = require('fs');

const async = require('async');
const csv = require('csv-stream');
const pg = require('pg');

const graphy = require('graphy');

let y_client = new pg.Client({
	user: 'blake_script',
	database: 'usgs',
	password: 'pass',
	host: 'localhost',
});

let k_queue = async.queue((s_query, fk_done) => {
	y_client.query(s_query, (e_query, h_result) => {
		if(h_result.rows.length) {
			debugger;
		}
	});
}, 1);

y_client.connect(function(e_connect) {
	if(e_connect) {
		throw new Error(`could not connect to database: `+e_connect);
	}

	y_client.query(`select *, st_asewkt(geom) as wkt from dbp where geom is not null and label is not null`, (e_query_dbp, h_result) => {
		if(e_query_dbp) {
			throw new Error(e_query_dbp);
		}

		h_result.rows.forEach((h_row) => {

			let s_query = `select a_uri, '${h_row.uri.replace(/'/g, "''")}' from (
					select
						a.geom a_geom,
						a.label a_label,
						a.uri a_uri,
						st_envelope(st_buffer(st_geographyfromtext('srid=4326;${h_row.wkt}'), 10000)::geometry) b_bb
					from gnis a
				) x
				where x.a_geom && x.b_bb
				and levenshtein('${h_row.label.replace(/'/g, "''")}', x.a_label, 3, 5, 10) < 10
			`;

			// let s_query = `select * from gnis a where
			// 	st_distance(a.geog, st_geographyfromtext('srid=4326;${h_row.wkt}'), false) < 100000
			// 	and levenshtein('${h_row.label.replace(/'/g, "''")}', a.label, 3, 5, 10) < 10
			// `;

			k_queue.push(s_query);
		});
	});
});



// select *,
//      st_distance(st_geogfromtext('srid=4326; point(""" + str(queryResult[i][1]) + " " + str(queryResult[i][0]) + """)'),geog,false) as distance,
//      levenshtein('""" + str(queryResult[i][2])+ """', name,3,5,10) as edit_distance
// from tgn_place_entity
// ) sub
// where sub.distance<100000 and sub.edit_distance<10
