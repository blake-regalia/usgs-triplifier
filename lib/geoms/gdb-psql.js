// native imports
const child_process = require('child_process');
const fs = require('fs');
const path = require('path');

// third-party includes
const classer = require('classer');
const mkdirp = require('mkdirp');
const ogr2ogr = require('ogr2ogr');
const unzip = require('unzip');
const replacestream = require('replacestream');

//
const unzipParse = unzip.Parse;


//
const local = classer.logger('gdb_psql');

//
let a_input_zips = process.argv.slice(2);

//
a_input_zips.filter((s_file) => {
	if('.zip' !== path.extname(s_file)) {
		local.fail(`only compressed geodatabase files are accepted (*.zip);\nrejecting: ${s_file}`);
		return false;
	}
	return true;
}).forEach((s_zip_file) => {
	let p_zip_file = path.join(process.cwd(), s_zip_file);

	// extract basename from zip file
	let s_basename = path.basename(s_zip_file, '.zip');

	// resolve data root directory
	let p_basename = path.join(p_zip_file, '..', s_basename);

	// unzipped geodatabase directory
	let p_gdb_dir = p_basename+'.gdb';
	let p_shp_dir = p_basename+'.shp';
	let p_sql_dir = p_basename+'.sql';

	// path prefix of target files within geodatabase; cache length for quick substr
	let s_gdb_target_prefix = `FILEGDB_101/${s_basename}/${s_basename}.gdb/`;
	let n_gdb_target_prefix = s_gdb_target_prefix.length;


	// unzip file into geodatabase
	function zip_gdb() {

		// mkdir of geodatabase
		mkdirp.sync(p_gdb_dir);

		// extract zip contents
		fs.createReadStream(p_zip_file)
			// extract
			.pipe(unzipParse())
			// each file in zip
			.on('entry', (k_file) => {

				// ref path of file relative to zip root
				let p_file = k_file.path;

				// file is contained under the geodatabase directory
				if(p_file.startsWith(s_gdb_target_prefix)) {
					// trim target prefix from beginning of path
					let s_file = p_file.substr(n_gdb_target_prefix);

					// file is a directory
					if('directory' === k_file.type.toLowerCase()) {
						// not just the root directory
						if(s_file.length) {
							local.fail(`did not expect geodatabase to have subdirectory "${s_file}"`);
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

				// all other cases, drain this file from buffer
				k_file.autodrain();
			})
			.on('close', () => {
				// continue
				gdb_shp();
			});
	}


	// convert geodatabase to shapefiles
	function gdb_shp() {

		// synchronously spawn dbp to shp conversion
		let a_gdb_shp_args = ['-f', 'ESRI Shapefile', p_shp_dir, p_gdb_dir];
		let u_dbp_shp = child_process.spawn('ogr2ogr', a_gdb_shp_args);

		// set encoding on stdout/stderr
		u_dbp_shp.stdout.setEncoding('utf8');
		u_dbp_shp.stderr.setEncoding('utf8');

		// stderr data event
		u_dbp_shp.stderr.on('data', (s_chunk) => {
			local.warn(s_chunk);
		});

		// error
		u_dbp_shp.on('error', (e) => {
			local.fail(e);
		});

		// process closes
		u_dbp_shp.on('close', () => {
			// delete gdb directory
			child_process.execSync(`rm -rf ${p_gdb_dir}`);

			// read shape file directory
			fs.readdir(p_shp_dir, (e_readdir, a_files) => {
				// each shape file
				a_files.forEach((s_file) => {
					if('.shp' === path.extname(s_file)) {
						shp_pgsql(p_shp_dir+'/'+s_file);
					}
				});
			});

			// before exit
			process.on('exit', () => {
				child_process.execSync(`rm -rf ${p_shp_dir}`);
			});
		});
	}


	// convert shapefile to postgis
	function shp_pgsql(p_shp_file) {

		//
		let p_output_dir = p_sql_dir;

		// mkdir of output
		mkdirp.sync(p_output_dir);

		let p_output_file = p_output_dir+'/'+path.basename(p_shp_file, '.shp')+'.sql';

		// synchronously spawn shp to pgsql conversion
		let a_shp_pgsql_args = [
			// '-a',  // appends shape file into current table
			'-d',  // create new table
			'-D',  // postgresql dump format (more compact)
			'-S',  // simple geometries (instead of MULTI)
			'-w',  // wkt format (drops M)
			'-I',  // create a spatial index on the geometry column
			p_shp_file,  // shapfile
			path.basename(p_shp_file, '.shp'),  // schema.table
		];
		let u_shp_pgsql = child_process.spawn('shp2pgsql', a_shp_pgsql_args);

		// set encoding on stdout/stderr
		u_shp_pgsql.stdout.setEncoding('utf8');
		u_shp_pgsql.stderr.setEncoding('utf8');

		// stderr data event
		u_shp_pgsql.stderr.on('data', (s_chunk) => {
			local.warn(s_chunk);
		});

		// stdout data event
		u_shp_pgsql.stdout
			// .pipe(replacestream(/create table "([^"]+)" \(([^;]+?)\);\s+alter table "([^"]+)" add primary key \(([^\)]+)\);\s+select AddGeometryColumn\('','[^']*','([^']*)','([^']*)','([^']*)',\d+\);/i,
			// 	'create table if not exists "$1" ($2, $5 geometry($7, $6), constraint pk_$3 primary key ($4));'))
			// .pipe(replacestream(/create index on "([^"]+)"/i, (_, s) => `create index if not exists "gix_${s}" on "${s}"` ))
			// .pipe(replacestream(/begin;\n|commit;\n/gi, ''))
			// .pipe(replacestream(/(\d+) 0 0(,|\))/g, '$1$2'))
			.pipe(replacestream(/\nDROP TABLE "([^"]+)";/, '\ndrop table if exists "$1" cascade;'))
			.pipe(fs.createWriteStream(p_output_file));

		// error
		u_shp_pgsql.on('error', (e) => {
			local.fail(e);
		});

		// process closes
		u_shp_pgsql.on('close', () => {
			// remove empty output files
			if(0 === fs.statSync(p_output_file).size) {
				fs.unlinkSync(p_output_file);
				local.warn('deleting '+p_output_file+' because it is empty');
			}
		});
	}

	// start pipeline
	zip_gdb();
});
