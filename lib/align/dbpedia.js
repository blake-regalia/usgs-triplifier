const fs = require('fs');
const graphy = require('graphy');

let pg = require('pg');

let ds_input = fs.createReadStream('../data-old/input/dbpedia/geonames_links.ttl');

let P_OWL_SAMEAS = 'http://www.w3.org/2002/07/owl#sameAs';
let R_GEONAMES_ORG = /http:\/\/sws\.geonames\.org\/(.+)\/$/;

// let y_pool = pg.Pool({max:require('os').cpus().length});

let N_BATCH_SIZE = 256;
let a_batch = [];

(async function () {
	// let drain_batch = async () => {
	// 	let a_ref = a_batch;
	// 	a_batch = [];
	// 	let h_rows = await y_pool.query(/* syntax: sql */ `
	// 		select gnis_id, geonames_id from
	// 		from gnis_sameas_geonames
	// 		where geonames_id in (${a_batch.map(s => `'${s}'`).join(',')})
	// 	`);
	// 	debugger;
	// 	h_rows;
	// };

	graphy.ttl.deserializer(ds_input, {
		async data(g_quad) {
			if(P_OWL_SAMEAS === g_quad.predicate.value && g_quad.object.isNamedNode && g_quad.subject.isNamedNode) {
				let m_geonames = R_GEONAMES_ORG.exec(g_quad.object.value);
				if(m_geonames) {
					let s_geonames_id = m_geonames[1];
					console.log([
						g_quad.subject.value,
						s_geonames_id,
					].join('\t'));

					// // reached batch size
					// if(N_BATCH_SIZE === a_batch.push(s_geonames_id)) {
					// 	this.pause();
					// 	drain_batch();
					// 	this.resume();
					// }
				}
			}
		},

		end() {},
	});
})();
