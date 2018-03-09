const fs = require('fs');
const cp = require('child_process');
const path = require('path');
const mkdirp = require('mkdirp');

require('colors');
const csv_parse = require('csv-parse');

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

					mkdirp(pd_save);
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
				});
			}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve())
				.then(() => {
					fk_file();
				});
		});
	});
}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());
