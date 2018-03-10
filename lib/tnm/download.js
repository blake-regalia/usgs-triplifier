const cp = require('child_process');
const path = require('path');

require('colors');
const mkdirp = require('mkdirp');
const request = require('request');
const csv_parse = require('csv-parse');

const PD_DOWNLOADS = path.join(__dirname, '../../data/input/');

const H_DATASETS = {
	nhd: {
		datasets: 'National Hydrography Dataset (NHD) Best Resolution',
		prodFormats: 'FileGDB 10.1',
		prodExtents: 'State',
	},
};

// process args are dataset names
let a_datasets = process.argv.slice(2);

// no datasets provided, default to all
if(!a_datasets.length) a_datasets = Object.keys(H_DATASETS);

// each dataset
a_datasets.map((s_dataset) => {
	return new Promise((fk_dataset) => {
		// download targets
		request({
			uri: 'https://viewer.nationalmap.gov/tnmaccess/api/products',
			qs: Object.assign({
				bbox: [
					'-136.23046875000003',
					'9.275622176792112',
					'-57.74414062500001',
					'61.56457388515458',
				].join(','),
				q: '',
				start: '',
				end: '',
				dateType: '',
				polyCode: '',
				polyType: '',
				reqPays: false,
				offset: 0,
				max: 5000,
				outputFormat: 'CSV',
			}, H_DATASETS[s_dataset]),
		}).pipe(
			// parse csv rows
			csv_parse({
				columns: true,
				skip_empty_lines: true,
			}, (e_parse, a_rows) => {
				// each row
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
					// done with dataset
					.then(() => {
						fk_dataset();
					});
			})
		);
	});
}).reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());
