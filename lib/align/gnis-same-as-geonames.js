const fs = require('fs');
const pg = require('pg');
const graphy = require('graphy');
const csv_parser = require('../util/csv-parser.js');

const progress = require('progress');
const worker = require('worker');


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


let k_group = worker.group('./workers/align.js');


// mk progress bar
let k_bar = new progress('[:bar] :percent :spin :mib_read MiB; +:elapseds; -:etas', {
	incomplete: ' ',
	complete: '∎', // 'Ξ',
	width: 40,
	total: fs.statSync(p_file).size,
});


let ds_csv = fs.createReadStream(p_file);

let a_queries = [];


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
		await k_group.data(a_queries)
			.map('query')
			.each((n_matches) => {
				c_matches += n_matches;
			})
			.end();

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

		// kill workers
		y_pool.kill();
	},
}));
