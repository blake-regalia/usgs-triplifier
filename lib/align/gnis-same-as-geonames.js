const fs = require('fs');
const pg = require('pg');
const graphy = require('graphy');
const csv_parser = require('../util/csv-parser.js');

const progress = require('progress');


const A_SPIN = ['◜ ◝', ' ˉ◞', ' ˍ◝', '◟ ◞', '◜ˍ ', '◟ˉ '];

let c_bytes = 0;
let n_update_bytes = 0;
let i_spin = 0;
let n_spin = A_SPIN.length;
let c_updates = 0;


let c_rows = 0;
let i_ins = 0;
let a_inserts = [];
let a_values = [];


let y_pool = new pg.Pool({max:require('os').cpus().length});

let p_file = process.argv[2];

// mk progress bar
let k_bar = new progress('[:bar] :percent :spin :mib_read MiB; +:elapseds; -:etas', {
	incomplete: ' ',
	complete: '∎', // 'Ξ',
	width: 40,
	total: fs.statSync(p_file).size,
});


let ds_csv = fs.createReadStream(p_file);

let a_queries = [];

const F_SORT_DISTANCE = (h_a, h_b) => {
	return h_a.distance - h_b.distance;
};

let k_serializer = graphy.ttl.serializer({
	coercions: new Map([]),
	prefixes: require('../../config.app.js').prefixes,
});
k_serializer.pipe(fs.createWriteStream('./data/output/geonames/same-as-geonames.ttl'));
let k_writer = k_serializer.writer;

// parse csv
ds_csv.pipe(csv_parser({
	delimiter: '|',

	async progress(n_bytes) {
		c_bytes += n_bytes;
		n_update_bytes += n_bytes;
		if(0 === (c_updates++ % 2)) {
			k_bar.tick(n_update_bytes, {
				mib_read: (c_bytes / 1024 / 1024).toFixed(2),
				spin: c_bytes === k_bar.total? ' ✓ ': A_SPIN[i_spin++],
			});

			i_spin = i_spin % n_spin;
			n_update_bytes = 0;
		}

		let c_matches = 0;
		await Promise.all(a_queries.map((a_query) => {
			return new Promise(async (fk_) => {
				let h_res = await y_pool.query(a_query[0], a_query[1]);
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

					c_matches += 1;

					// console.log(`${a_query[1]} -- MATCH --`);
				}

				fk_();
			});
		})); //.reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());

		// console.info(`${c_matches} matches / ${a_queries.length} attempts made (${(1e2*c_matches/a_queries.length).toFixed(2)}%)`);
	},

	// each row
	row(h_row) {
		a_queries.push([
			/* syntax: sql */ `
				select
					id,
					alternate_names,
					st_distance(
						geom::geography,
						st_geomfromewkt(
							'SRID=4326;POINT(${h_row.prim_long_dec} ${h_row.prim_lat_dec})'
							)::geography,
							true
						) as distance
				from geonames
				where name = $1
					and st_distance(geom::geography, st_geomfromewkt('SRID=4326;POINT(${h_row.prim_long_dec} ${h_row.prim_lat_dec})')::geography, true) < 10000
					-- and st_distance(geom, st_geomfromewkt('SRID=4326; POINT(${h_row.prim_long_dec} ${h_row.prim_lat_dec})')) < .0015
		`, [h_row.feature_name.trim().replace(/\s*\(historical\)$/, '')], h_row.feature_id.replace(/^0+/, '') || '0']);
	},

	// end of csv input
	end() {
		k_bar.tick(n_update_bytes, {
			mib_read: (c_bytes / 1024 / 1024).toFixed(2),
			spin: c_bytes === k_bar.total? ' ✓ ': A_SPIN[i_spin++],
		});

		y_pool.end();
	},
}));
