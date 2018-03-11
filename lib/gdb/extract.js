
const fs = require('fs');
const path = require('path');

require('colors');
const mkdirp = require('mkdirp');
const ogr2ogr = require('ogr2ogr');
const pg = require('pg');
const unzip_parse = require('unzip').Parse;

function usage() {
	throw new Error('usage: extract TRIPLIFIER INPUTS...');
}

// triplifier
let p_triplifier = process.argv[2];
if(!p_triplifier.endsWith('.js')) usage();
let triplify = require(path.join(process.cwd(), p_triplifier));

// prep postgres config string
let s_postgres_config = ((h) => {
	return `host='${h.PGHOST || 'postgis'}'
			port='${h.PGPORT || '25432'}'
			dbname='${h.PGDATABASE || 'gis'}'
			user='${h.PGUSER || 'docker'}'
			password='${h.PGPASSWORD || 'docker'}'
	`.replace(/\t/g, '').replace(/\n/g, ' ');
})(process.env);

// database client
let y_client = new pg.Client();
y_client.connect();

// process geodatabase into postgres
async function gdb(p_input) {
	// clear tables
	await y_client.query('drop schema public cascade');
	await y_client.query('create schema public');

	// enable postgis
	await y_client.query('create extension postgis');

	console.log(`importing ${path.basename(p_input)} into PostgreSQL...`.blue);

	return new Promise((fk_ogr) => {
		// process
		let ds_ogr = ogr2ogr(p_input)
			.format('PostgreSQL')
			.timeout(20*60*1000)  // 20 minutes
			.options([
				'-unsetFid',
				'-progress',
				'-lco', 'GEOM_TYPE=geometry',  // same as default
				'-lco', 'DIM=2',  // 2D geometries
				'--config', 'PG_USE_COPY', 'YES',  // faster import
			])
			.destination(`PG:${s_postgres_config}`)
			.stream();

		// error
		ds_ogr.on('error', (e_ogr) => {
			throw e_ogr;
		});

		// close
		ds_ogr.on('close', () => {
			fk_ogr();
		});

		// pipe progress to stdout
		ds_ogr.pipe(process.stdout);
	});
}

// each input
process.argv.slice(3).map((s_input) => async () => {
	// normalize file path
	let p_input = path.join(process.cwd(), s_input);

	// zip file
	if(p_input.endsWith('.zip')) {
		// extract basename from zip file
		let s_basename = path.basename(p_input, '.zip');

		// resolve geodatabase directory location
		let p_gdb_dir = path.join(p_input, '..', s_basename)+'.gdb';

		// path prefix of target files within geodatabase; cache length for quick substr
		let s_gdb_target_prefix = `FILEGDB_101/${s_basename}/${s_basename}.gdb/`;
		let n_gdb_target_prefix = s_gdb_target_prefix.length;

		let s_gdb_target_match = s_basename+'.gdb/';
		let n_gdb_target_match = s_gdb_target_match.length;

		// mkdir of geodatabase
		mkdirp.sync(p_gdb_dir);

		// async
		return new Promise((fk_extract) => {
			// extract zip contents
			fs.createReadStream(p_input)
				// extract
				.pipe(unzip_parse())

				// invalid zip
				.on('error', (e_unzip) => {
					console.warn(`cannot read zip file: '${s_input}':\n${e_unzip.message}`);

					// next file
					fk_extract();
				})

				// each file in zip
				.on('entry', (k_file) => {
					// ref path of file relative to zip root
					let p_file = k_file.path;
					let b_directory = 'directory' === k_file.type.toLowerCase();

					// file is contained under the geodatabase directory
					if(p_file.startsWith(s_gdb_target_prefix)) {
						// trim target prefix from beginning of path
						let s_file = p_file.substr(n_gdb_target_prefix);

						// file is a directory
						if(b_directory) {
							// not just the root directory
							if(s_file.length) {
								throw new Error(`did not expect geodatabase to have subdirectory "${s_file}"`);
							}
						}
						else {
							// prep path of output file
							let p_output_file = `${p_gdb_dir}/${s_file}`;

							// write to output
							k_file.pipe(
								fs.createWriteStream(p_output_file)
							);

							// do not autodrain this file
							return;
						}
					}
					// directly here
					else if(p_file.startsWith(s_gdb_target_match) && !b_directory) {
						// trim target prefix from beginning of path
						let s_file = p_file.substr(n_gdb_target_match);

						// prep path of output file
						let p_output_file = `${p_gdb_dir}/${s_file}`;

						// write to output
						k_file.pipe(
							fs.createWriteStream(p_output_file)
						);

						// do not autodrain this file
						return;
					}
					else if(!b_directory) {
						console.warn(`skipping file: ${p_file}`.yellow);
					}

					// all other cases, drain this file from buffer
					k_file.autodrain();
				})

				// done reading archive
				.on('close', async () => {
					// continue
					await gdb(p_gdb_dir);

					// delete gdb directory
					require('child_process').execSync(`rm -rf ${p_gdb_dir}`);

					// triplify database
					await triplify(s_basename);

					// next input
					fk_extract();
				});
		});
	}
	// geodatabase
	else if(p_input.endsWith('.gdb')) {
		// process geodatabase
		await gdb(p_input);

		// triplify databse
		await triplify(path.basename(p_input));
	}
	// invalid
	else {
		usage();
	}
})
	// thru async each
	.reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve())
	.then(() => {
		// close pg connection
		y_client.end();
	});

