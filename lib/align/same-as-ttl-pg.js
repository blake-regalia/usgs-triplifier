const fs = require('fs');
const pg = require('pg');
const graphy = require('graphy');
const progress = require('progress');

debugger;
let ds_input = fs.createReadStream(process.argv[2]);

let R_GEONAMES_ORG = /http:\/\/sws\.geonames\.org\/(.+?)\/?$/;

let y_pool = new pg.Pool({max:require('os').cpus().length});

const N_BATCH = 1024;

let a_geonames = [];
let c_matches = 0;


const A_SPIN = ['◜ ◝', ' ˉ◞', ' ˍ◝', '◟ ◞', '◜ˍ ', '◟ˉ '];
let i_spin = 0;

let c_drains = 0;
let b_final = false;

let ds_out = fs.createWriteStream(process.argv[3]);

(async function () {
	// mk progress bar
	let y_bar = new progress('[:bar] :percent :spin; +:elapseds; -:etas', {
		incomplete: ' ',
		complete: '∎', // 'Ξ',
		width: 40,
		// total: +(await y_pool.query('select count(*) from dbpedia_same_as_geonames')).rows[0].count,
		total: 4029299,
	});

	async function drain() {
		c_drains += 1;
		let a_ref = a_geonames;
		a_geonames = [];
		let h_res = await y_pool.query(/* syntax: sql */ `
			select dbr
			from dbpedia_same_as_geonames
			where geonames_id in (${a_ref.map(g => `'${g.geonames}'`).join(',')})
		`);

		c_matches += h_res.rowCount;

		y_bar.tick(a_ref.length, {
			spin: A_SPIN[i_spin++],
		}); i_spin = i_spin % A_SPIN.length;

		h_res.rows.forEach((h_row) => {
			ds_out.write(h_row.dbr+'\n');
		});

		c_drains -= 1;

		// finally
		if(b_final && !c_drains) {
			ds_out.end();
			console.log(c_matches);
		}
	}

	graphy.ttl.deserializer(ds_input, {
		async data(g_quad) {
			let [, s_geonames_id] = R_GEONAMES_ORG.exec(g_quad.object.value);
			let p_gnisf = g_quad.subject.value;
			if(N_BATCH === a_geonames.push({gnisf:p_gnisf, geonames:s_geonames_id})) {
				drain();
			}
		},

		async end() {
			b_final = true;
			drain();
		},
	});
})();
