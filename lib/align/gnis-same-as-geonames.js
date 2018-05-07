const fs = require('fs');
const pg = require('pg');
const progress = require('progress');
const graphy = require('graphy');
const splitter = require('../pg/splitter.js');

const N_ROWS_PER_READ = 1 << 9;


let y_pool = new pg.Pool({max:require('os').cpus().length});

const H_PREFIXES = require('../../config.app.js').prefixes;
let k_serializer = graphy.ttl.serializer({
	coercions: new Map(),
	prefixes: H_PREFIXES,
});

const P_GEONAMES_ORG = H_PREFIXES.geonames;

k_serializer.pipe(fs.createWriteStream('./data/output/gnis/same-as-geonames.ttl'));
let k_writer = k_serializer.writer;

(async function() {
	let n_gnis_features = +(await y_pool.query('select count(*) from gnis')).rows[0].count;

	const A_SPIN = ['◜ ◝', ' ˉ◞', ' ˍ◝', '◟ ◞', '◜ˍ ', '◟ˉ '];
	let i_spin = 0;

	// mk progress bar
	let y_bar = new progress('[:bar] :percent :spin; +:elapseds; -:etas', {
		incomplete: ' ',
		complete: '∎', // 'Ξ',
		width: 40,
		total: n_gnis_features,
	});

	await splitter({
		pool: y_pool,
		rows: n_gnis_features,
		cores: require('os').cpus().length*4,
		updates: 1 << 8,

		query: (n_limit, i_offset) => /* syntax: sql */ `
			select
				i.gnis_id,
				i.geonames_id,
				st_distance(i.gnis_geog, i.geonames_geog, true) as distance
			from (
				select
					a.gnis_id,
					a.gnis_geog,
					b.id geonames_id,
					b.geom::geography geonames_geog
				from (
					select
						name,
						id gnis_id,
						geom::geography gnis_geog
					from gnis
					order by id
					limit ${n_limit} offset ${i_offset}
				) a
				join geonames b
					on a.name = b.name
			) i
			order by distance asc
		`,

		context(n_limit) {
			// hash of gnis already seen throughout this query
			let h_gnis_seen = {};
			let c_features_added = 0;

			return [(a_rows) => {
				let c_features = 0;
				let b_kill = true;
				kill:
				for(;;) {
					// each row under cursor
					for(let h_row of a_rows) {
						let s_gnis_id = h_row.gnis_id;

						// too far (more than 10km); stop searching
						if(h_row.distance > 10e3) break kill;

						// already encountered this gnis feature; try remainders
						if(s_gnis_id in h_gnis_seen) continue;

						// seen it now
						h_gnis_seen[s_gnis_id] = 1;

						// make same-as relation
						k_writer.add({
							[`gnisf:${s_gnis_id}`]: {
								'owl:sameAs': [`>${P_GEONAMES_ORG}${h_row.geonames_id}/`],
							},
						});

						c_features += 1;

						// reached limit; stop searching
						if(++c_features_added === n_limit) break kill;
					}

					b_kill = false;
					break;
				}

				y_bar.tick(c_features, {
					spin: A_SPIN[i_spin++],
				}); i_spin = i_spin % A_SPIN.length;

				return b_kill;
			}, () => {
				// console.log(`\n${c_features_added} / ${n_limit}`);

				y_bar.tick(n_limit - c_features_added, {
					spin: A_SPIN[i_spin++],
				});

				i_spin = i_spin % A_SPIN.length;
			}];
		},
	});

	// close serializer
	k_serializer.close();

	// end pg pool
	y_pool.end();
})();
