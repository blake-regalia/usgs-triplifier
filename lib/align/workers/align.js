const fs = require('fs');
const pg = require('pg');
const graphy = require('graphy');
const worker = require('worker');


const F_SORT_DISTANCE = (h_a, h_b) => {
	return h_a.distance - h_b.distance;
};


let y_client = new pg.Client();
y_client.connect();


let k_serializer = graphy.ttl.serializer({
	coercions: new Map([]),
	prefixes: require('../../../config.app.js').prefixes,
});
k_serializer.pipe(fs.createWriteStream(`./data/output/geonames/same-as-geonames-${process.env.WORKER_INDEX}.ttl`));
let k_writer = k_serializer.writer;

worker.dedicated({
	async query(a_queries) {
		let c_matches = 0;

		for(let a_query of a_queries) {
			let h_res = await y_client.query(a_query[0], a_query[1]);
			let n_matches = h_res.rowCount;
			if(!n_matches) {
				// console.warn(`${a_query[1]}  xx NOT FOUND xx`);
			}
			else {
				let h_match;
				if(n_matches > 1) {
					let a_sorted = h_res.rows.sort(F_SORT_DISTANCE);
					h_match = a_sorted[0];
				}
				else {
					h_match = h_res.rows[0];
				}

				k_writer.add({
					[`gnisf:${a_query[2]}`]: {
						'owl:sameAs': [`geonames:${h_match.id}`],
					},
				});

				// console.log(`${a_query[1]} -- MATCH --`);
				c_matches += 1;
			}
		}

		return c_matches;
	},

	end() {
		k_serializer.close();
	},
});
