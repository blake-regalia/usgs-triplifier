const fs = require('fs');
const cp = require('child_process');
const path = require('path');

require('colors');
const csv_parse = require('csv-parse');
const request = require('request');
const request_progress = require('request-progress');
const progress = require('progress');

const PD_DOWNLOADS = path.join(__dirname, '../../data/input/');

process.argv.slice(2).map((p_file) => {
	return new Promise((fk_file) => {
		let s_contents = fs.readFileSync(p_file);

		csv_parse(s_contents, {
			columns: true,
			skip_empty_lines: true,
		}, (e_parse, a_rows) => {
			a_rows.map((h_row) => async () => {
				await new Promise((fk_download) => {
					let p_download = h_row.downloadURL;

					let s_name = path.basename(p_download);
					console.log(`${s_name}...`);
					let s_category = s_name.replace(/^([a-zA-Z]+)_.+$/, '$1').toLowerCase() || 'other';
					let pd_save = path.join(PD_DOWNLOADS, s_category);

					let du_curl = cp.spawn('curl', ['-O', p_download], {
						cwd: pd_save,
					});
					du_curl.stdout.pipe(process.stdout);
					du_curl.stderr.pipe(process.stderr);
					du_curl.on('close', (nc_exit) => {
						if(nc_exit) {
							console.error(`Failed to download ${s_name}`);
						}

						fk_download();
					});

					// let a_spin = ['◜ ◝', ' ˉ◞', ' ˍ◝', '◟ ◞', '◜ˍ ', '◟ˉ '];
					// let i_spin = 0;
					// let n_spin = a_spin.length;

					// let y_bar = new progress('[:bar] :percent :spin :mib MiB; +:elapseds; -:etas', {
					// 	incomplete: ' ',
					// 	complete: '∎', // 'Ξ',
					// 	width: 40,
					// 	total: 1,
					// });

					// request_progress(request(p_download))
					// 	.on('progress', (g_state) => {
					// 		let {
					// 			size: {
					// 				total: nb_total,
					// 				transferred: nb_tx,
					// 			},
					// 		} = g_state;

					// 		y_bar.curr = nb_tx;
					// 		y_bar.total = nb_total;
					// 		y_bar.render({
					// 			mib: (nb_tx / 1048576).toFixed(2),
					// 			spin: a_spin[i_spin++],  // ' ✓ '
					// 		});

					// 		// modulate spinner
					// 		i_spin = i_spin % n_spin;
					// 	})
					// 	.pipe(fs.createWriteStream(p_save))
					// 	.on('finish', () => {
					// 		// final update
					// 		// let nb_read = y_bar.curr = ;
					// 		y_bar.render({
					// 			mib: (nb_read / 1048576).toFixed(2),
					// 			spin: ' ✓ ',
					// 		});

					// 		console.log('\n');
					// 		fk_download();
					// 	});
				});
			}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve())
				.then(() => {
					fk_file();
				});
		});
	});
}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());

