
const path = require('path');
const fs = require('fs');
const readline = require('readline');

const async = require('async');
const csv = require('csv-stream');
const pg = require('pg');

const graphy = require('graphy');

let y_client = new pg.Client(Object.assign(require(__dirname+'/../../postgres-config.json'), {
	database: 'usgs_dbp',
}));

let ds_sames = fs.createWriteStream(__dirname+'/../../data/output/gnis/same-as.ttl');

ds_sames.write(`
	@prefix gnisf: <http://data.usgs.gov/lod/gnis/feature/> .
	@prefix owl: <http://www.w3.org/2002/07/owl#> .
`);


let k_queue = async.queue((h_mapping, fk_done) => {
	y_client.query(h_mapping.query, (e_query, h_result) => {
		if(e_query) throw e_query;
		if(h_result.rows.length) {
			let s_dbr = h_result.rows[0].uri;
			console.info(h_mapping.feature+' sameAs '+s_dbr);
			ds_sames.write(`${h_mapping.feature} owl:sameAs <${s_dbr}> .\n`);
		}
		fk_done();
	});
}, 1);

let b_ended = false;
k_queue.drain = () => {
	if(b_ended) ds_sames.end();
};

y_client.connect(function(e_connect) {
	if(e_connect) {
		throw new Error(`could not connect to database: `+e_connect);
	}
	// debugger;

	let dr_input = readline.createInterface({
		input: fs.createReadStream(__dirname+'/../../data/output/gnis/geoms.tsv'),
	});

	dr_input.on('line', (s_line) => {
		let [, s_wkt, s_label, s_tt_feature, s_class] = s_line.split('\t');
		// switch(s_class) {
		// 	case 'Populated Place': {

		let [, s_state, , , s_name] = s_tt_feature.split(/[:\.]/g);
		// debugger;
		k_queue.push({
			feature: s_tt_feature,
			query: `select * from dbp where uri = 'http://dbpedia.org/resource/${s_name},_${s_state}'`,
		});

				// break;
		// 	}
		// }
	});

	dr_input.on('close', () => {
		b_ended = true;
	});


	// y_client.query(`select *, st_asewkt(geom) as wkt from dbp where geom is not null and label is not null`, (e_query_dbp, h_result) => {
	// 	if(e_query_dbp) {
	// 		throw new Error(e_query_dbp);
	// 	}

	// 	h_result.rows.forEach((h_row) => {

	// 		let s_query = `select a_uri, '${h_row.uri.replace(/'/g, "''")}' from (
	// 				select
	// 					a.geom a_geom,
	// 					a.label a_label,
	// 					a.uri a_uri,
	// 					st_envelope(st_buffer(st_geographyfromtext('srid=4326;${h_row.wkt}'), 10000)::geometry) b_bb
	// 				from gnis a
	// 			) x
	// 			where x.a_geom && x.b_bb
	// 			and levenshtein('${h_row.label.replace(/'/g, "''")}', x.a_label, 3, 5, 10) < 10
	// 		`;

	// 		// let s_query = `select * from gnis a where
	// 		// 	st_distance(a.geog, st_geographyfromtext('srid=4326;${h_row.wkt}'), false) < 100000
	// 		// 	and levenshtein('${h_row.label.replace(/'/g, "''")}', a.label, 3, 5, 10) < 10
	// 		// `;

	// 		k_queue.push(s_query);
	// 	});
	// });
});



// select *,
//      st_distance(st_geogfromtext('srid=4326; point(""" + str(queryResult[i][1]) + " " + str(queryResult[i][0]) + """)'),geog,false) as distance,
//      levenshtein('""" + str(queryResult[i][2])+ """', name,3,5,10) as edit_distance
// from tgn_place_entity
// ) sub
// where sub.distance<100000 and sub.edit_distance<10
