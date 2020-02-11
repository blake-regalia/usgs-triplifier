
const path = require('path');
const fs = require('fs');

require('colors');
const pg = require('pg');
const factory = require('@graphy/core.data.factory');
const ttl_write = require('@graphy/content.ttl.write');
const mkdirp = require('mkdirp');
const worker = require('worker');

const app_config = require('../../config.app.js');

Object.assign(global, require('./symbols.js'));
/* globals
H_CODES

HPG_POINT
HPG_MULTIPOINT
HPG_LINESTRING
HPG_MULTILINESTRING
HPG_POLYGON
HPG_MULTIPOLYGON

H_TABLES_FEATURES

id_to_ct
code_to_ct

*/

const N_CORES = require('os').cpus().length;


const AS_SKIP = new Set([
	'spatial_ref_sys',
]);

const H_TABLES_ATTRIBUTE = {
};

const H_TABLES_STATIC = {
	nhdfcode: {
		rename: 'fcode',
		key: 'fcode',
		transform: fcodes,
	},
};

const H_TABLES_META = {
	nhdflow: {
		// rename: 'flow',
		// key: 'from_permanent_identifier',
		// cardinality: 'many',
	},
	// nhdflowlineevaa: {
	// 	rename: 'flowline_vaa',
	// 	key: 'permanent_identifier',
	// },
	// nhdverticalrelationship: {
	// 	rename: 'vertical',
	// 	key: 'permanent_identifier',
	// },
	// externalcrosswalk: {
	// 	rename: 'crosswalk',
	// 	key: 'permanent_identifier',
	// },
	// // nhdprocessingparameters
	// nhdstatus: {
	// 	rename: 'status',
	// 	key: 'permanent_identifier',
	// },
	// nhdreachcodemaintenance: {
	// 	rename: 'reach_maintenance',
	// 	key: 'permanent_identifier',
	// },
	// nhdreachcrossreference: {
	// 	rename: 'reach_cross_reference',
	// 	key: 'oldreachcode',
	// },

	// nhdfeaturetometadata: {
	// 	rename: 'feature_to_metadata',
	// 	key: 'permanent_identifier',
	// },
	// nhdmetadata: {
	// 	rename: 'metadata',
	// 	key: 'meta_processid',
	// },
	// nhdsourcecitation: {
	// 	rename: 'citation',
	// 	key: 'meta_processid',
	// },
};


const clean = s => s.replace(/[\s-]/g, '_');

const H_FCODE_ATTRIBUTES = {
	// canalditchtype: {
	// 	predicate: 'rdfs:subClassOf',
	// 	object: s => 'nhd:Canal_or_Ditch',
	// },
	// pipelinetype: {
	// 	predicate: 'rdfs:subClassOf',
	// 	object: s => 'nhd:Pipeline',
	// },
	// reservoirtype: {
	// 	predicate: 'rdfs:subClassOf',
	// 	object: s => 'nhd:Reservoir',
	// },

	constructionmaterial: {
		predicate: 'nhd:constructionMaterial',
		object: s => `nhd:${clean(s)}_Construction_Material`,
	},
	hydrographiccategory: {
		predicate: 'nhd:hydrographicCategory',
		object: s => `nhd:Hydrographically_${clean(s)}`,
	},
	inundationcontrolstatus: {
		predicate: 'nhd:inundationControlStatus',
		object: s => `nhd:${clean(s)}_Inundation`,
	},
	operationalstatus: {
		predicate: 'nhd:operationalStatus',
		object: s => `nhd:${clean(s)}_Operational_Status`,
	},
	positionalaccuracy: {
		predicate: 'nhd:positionalAccuracy',
		object: s => `nhd:${clean(s)}_Positional_Accuracy`,
	},
	relationshiptosurface: {
		predicate: 'nhd:relationshipToSurface',
		object: s => `nhd:${clean(s)}_Surface_Relation`,
	},
	stage: {
		predicate: 'nhd:stage',
		object: s => `nhd:${clean(s)}_Stage`,
	},
};

function fcodes(h_fcodes, k_writer) {
	// make sure we don't create conflicting IRIs
	let as_iris = new Set();

	let h_scts = {};

	// fcode naming
	for(let s_fcode in h_fcodes) {
		let h_row = h_fcodes[s_fcode];

		// ref description
		let s_description = h_row.description;

		// create name suffix
		let s_suffix = s_description
			.replace(/(\w+)\/(\w+)/, '$1 or $2')
			.replace(/^([^:]+)\s+(\w+)=(\w+);(.*)$/, '$1: $2=$3; $4')
			.replace(/^([^:]+):\s*(.+)$/, (m_subtype, s_type, s_params) => {
				// start with primary type
				let s_rename = s_type;

				// form params hash
				let h_params = {};
				s_params.split(/;\s*/g).forEach((s_pair) => {
					let [s_key, s_value] = s_pair.split(/\s*=\s*/);
					h_params[s_key] = s_value;
				});

				// actual subtype
				let s_subtype = `${s_type} Type`;
				if(s_subtype in h_params) {
					s_rename = h_params[s_subtype]+' '+s_rename;
					delete h_params[s_subtype];
				}

				// consolidate other params
				for(let s_key in h_params) {
					let s_value = h_params[s_key];

					// prepend to primary
					s_rename = s_value+' '+s_rename;
				}

				return s_rename;
			})
			.replace(/[\s-]/g, '_');

		// conflict
		if(as_iris.has(s_suffix)) {
			throw new Error('NHD subtype iri conflict: '+s_suffix);
		}

		// add to set
		as_iris.add(s_suffix);

		// save to row
		h_scts[s_fcode] = `nhdf:${s_suffix}`;
	}

	// fcode triplification
	for(let s_fcode in h_fcodes) {
		let h_row = h_fcodes[s_fcode];

		// kcode
		let s_kcode = h_row.kcode;

		// form pairs
		let h_pairs = {
			'nhd:fcode': '"'+s_fcode,
			'nhd:description': '"'+h_row.description,
			'nhd:kcode': '"'+s_kcode,
		};

		// subclass
		if(s_kcode.includes(',')) {
			h_pairs['rdfs:subClassOf'] = `${h_scts[s_kcode.split(',')[0]+'00']}`;
		}

		// append
		for(let s_key in h_row) {
			let z_value = h_row[s_key];
			if('string' === typeof z_value) z_value = z_value.trim();
			if(z_value && s_key in H_FCODE_ATTRIBUTES) {
				let h_attribute = H_FCODE_ATTRIBUTES[s_key];
				h_pairs[h_attribute.predicate] = h_attribute.object(z_value);
			}
		}

		// write triples
		k_writer.add({
			[h_scts[s_fcode]]: h_pairs,
		});
	}

	return h_scts;
}

const split_query = (h_config) => {
	let {
		rows: n_rows,
		table: s_table,
		query: s_query,
		cores: n_cores=N_CORES,
	} = h_config;

	// size of each range
	let n_range_size = Math.ceil(n_rows / n_cores);

	// queries list
	let a_queries = [];

	// initiate lo/hi
	let i_lo = 0;
	let i_hi = n_range_size;

	// ranges
	let c_ranges = 0;

	// mk ranges
	while(i_lo < n_rows) {
		let n_chunk = i_hi - i_lo;

		// add range
		a_queries.push({
			index: c_ranges++,
			rows: n_chunk,
			table: s_table,
			query: `${s_query} order by ogc_fid asc limit ${n_chunk} offset ${i_lo}`,
		});

		// advance lo
		i_lo = i_hi;

		// increment hi
		i_hi = Math.min(i_hi+n_range_size, n_rows);
	}

	return a_queries;
};


const triplify = async (s_basename) => {
	// output directory
	let p_output_dir = `./data/output/nhd/${s_basename}`;
	mkdirp.sync(p_output_dir);

	// output files
	let p_triples = `${p_output_dir}/static.ttl`;

	// create serializer
	let kt_xsd_date = factory.namedNode('http://www.w3.org/2001/XMLSchema#date');
	let kt_xsd_integer = factory.namedNode('http://www.w3.org/2001/XMLSchema#integer');
	let ds_writer = ttl_write({
		prefixes: app_config.prefixes,
		coercions: new Map([
			[Date, dt => factory.literal(dt.toISOString().replace(/T.+$/, ''), kt_xsd_date)],
			[Number, x => factory.literal(x+'', kt_xsd_integer)],
		]),
	});

	// pipe output to file
	ds_writer.pipe(fs.createWriteStream(p_triples));

	// database client
	let y_client = new pg.Client();

	// connect
	await y_client.connect();

	// fetch tables (map rows to list of names)
	let a_tables = (await y_client.query(`select tablename as name from pg_tables where schemaname='public'`))
		.rows.map(({name:s_table}) => s_table);


	// static tables
	let h_static = {};
	await Promise.all(a_tables
		.filter(s => s in H_TABLES_STATIC)
		.map(s_table => (async () => {
			// fetch rows
			let a_rows = (await y_client.query(`select * from ${s_table}`)).rows;

			// print
			console.log(`triplifying ${a_rows.length} rows from static table ${s_table}`.blue);

			// attribute info
			let {
				rename: s_rename,
				key: s_key,
				cardinality: s_cardinality,
				transform: f_transform,
			} = H_TABLES_STATIC[s_table];

			// make hash
			let h_hash = {};

			// make each key a list
			if('many' === s_cardinality) {
				// save to hash
				a_rows.forEach((h_row) => {
					let s_group = h_row[s_key];

					// make list
					if(!(s_group in h_hash)) {
						h_hash[s_group] = [h_row];
					}
					// push item to list
					else {
						h_hash[s_group].push(h_row);
					}
				});
			}
			// singletons
			else {
				// save to hash
				a_rows.forEach((h_row) => {
					// under given key
					h_hash[h_row[s_key]] = h_row;
				});
			}

			// apply transform
			h_static[s_rename] = f_transform(h_hash, ds_writer);
		})()));


	// make workers
	let k_group = worker.group('./triplifier.js', null, {
		cwd: p_output_dir,
		// inspect: {
		// 	range: [9230, 9242],
		// 	brk: true,
		// },
	});

	// initialize them
	await k_group.run('init', [h_static]);


	// meta tables
	await Promise.all(a_tables
		.filter(s => s in H_TABLES_META)
		.map(s_table => (async () => {
			// count how many rows in this table
			let n_rows = +(await y_client.query(`select count(*) as count from ${s_table}`)).rows[0].count;

			// no rows
			if(!n_rows) {
				console.warn(`skipping meta table ${s_table} because it has 0 rows`.yellow);
				return;
			}

			// split query
			let a_queries = split_query({
				rows: n_rows,
				table: s_table,
				query: `select * from ${s_table}`,
			});

			// progress bar
			let y_bar = new (require('progress'))(`[:bar] :percent complete; :current/:total; -:etas; +:elapsed`, {
				total: n_rows,
			});

			// progress
			console.log(`triplifying ${n_rows} rows from meta table ${s_table}...`.blue);

			// assign task to group
			await k_group
				.use(a_queries)
				.map('triplify', [], {
					// progress event
					progress(i_subset, n_rows_update) {
						// update progress bar
						y_bar.tick(n_rows_update);
					},
				})
				.end();
		})()));


	// // start with attribute tables
	// let h_static = {};

	// // print
	// console.log('fetching rows from attirbute tables...'.blue);


	// // load each in parallel
	// await Promise.all(a_tables
	// 	.filter(s_table => s_table in H_TABLES_ATTRIBUTE)
	// 	.map(s_table => (async () => {
	// 		// fetch rows
	// 		let a_rows = (await y_client.query(`select * from ${s_table}`)).rows;

	// 		// attribute info
	// 		let {
	// 			rename: s_rename,
	// 			key: s_key,
	// 			cardinality: s_cardinality,
	// 		} = H_TABLES_ATTRIBUTE[s_table];

	// 		// make hash
	// 		let h_hash = h_static[s_rename] = {};

	// 		// make each key a list
	// 		if('many' === s_cardinality) {
	// 			// save to hash
	// 			a_rows.forEach((h_row) => {
	// 				let s_group = h_row[s_key];

	// 				// make list
	// 				if(!(s_group in h_hash)) {
	// 					h_hash[s_group] = [h_row];
	// 				}
	// 				// push item to list
	// 				else {
	// 					h_hash[s_group].push(h_row);
	// 				}
	// 			});
	// 		}
	// 		// singletons
	// 		else {
	// 			// save to hash
	// 			a_rows.forEach((h_row) => {
	// 				// under given key
	// 				h_hash[h_row[s_key]] = h_row;
	// 			});
	// 		}

	// 		console.log(` + ${s_table}`.green);
	// 	})()));

	// // process fcodes
	// fcodes(h_static.fcode, k_writer);


	// close attribute serializer
	ds_writer.end();



	// // write attribute data objects to file
	// let p_attributes = `${p_output_dir}/attributes.json`;
	// let dsk_attributes = json_stream.stringifyObject('{\n\t', ',\n\t', '\n}\n');
	// dsk_attributes.pipe(fs.createWriteStream(p_attributes));

	// // each attribute table
	// Object.keys(h_static).forEach((s_attribute) => {
	// 	dsk_attributes.write([s_attribute, h_static[s_attribute]]);

	// 	// free to gc
	// 	delete h_static[s_attribute];
	// });

	// // close attributes json
	// dsk_attributes.end();


	// each feature table
	await a_tables
		.filter(s => s in H_TABLES_FEATURES)
		.map(s_table => (async () => {
			let hpg_type = H_TABLES_FEATURES[s_table];

			// count how many rows in this table
			let n_features = +(await y_client.query(`select count(*) from ${s_table}`)).rows[0].count;

			// zero features; skip table
			if(!n_features) {
				console.warn(`skipping ${s_table} because it has 0 features`);
				return;
			}

			// assert correct geometry type (or multipart upgrade)
			let s_geometry_type = (await y_client.query(`select st_geometrytype(wkb_geometry) as geometry_type from ${s_table} limit 1`)).rows[0].geometry_type.replace(/^ST_/, '');
			if(s_geometry_type !== hpg_type.proper && (!hpg_type.multipart || 'Multi'+s_geometry_type !== hpg_type.proper)) {
				throw new Error(`geometry type mismatch on table ${s_table}; expected ${hpg_type.proper}, but geometry is of type ${s_geometry_type}`);
			}

			// progress bar
			let y_bar = new (require('progress'))(`[:bar] :percent complete; :current/:total; -:etas; +:elapsed`, {
				total: n_features,
			});

			// progress
			console.log(`triplifying ${n_features} features from ${s_table}...`.blue);

			// build query selections
			let a_select = [];
			if(HPG_POINT === hpg_type) {
				a_select.push(...[
					'st_astext(wkb_geometry) as wkt_geometry',
				]);
			}
			else {
				a_select.push(...[
					'st_astext(st_centroid(wkb_geometry)) as wkt_centroid',
					'st_astext(st_envelope(wkb_geometry)) as wkt_bounding_box',
					'st_npoints(wkb_geometry) as n_points',
				]);
			}

			if(hpg_type.linear) {
				a_select.push(...[
					'st_astext(st_startpoint(wkb_geometry)) as wkt_start_point',
					'st_astext(st_endpoint(wkb_geometry)) as wkt_end_point',
					'st_length(wkb_geometry::geography) as x_length',
				]);
			}

			if(hpg_type.areal) {
				a_select.push(...[
					'st_area(wkb_geometry::geography) as x_area',
					'st_perimeter(wkb_geometry::geography) as x_perimeter',
				]);
			}

			if(hpg_type.rings) {
				a_select.push(...[
					'st_nrings(wkb_geometry) as n_rings',
				]);
			}

			if(hpg_type.multipart) {
				a_select.push(...[
					'st_numgeometries(wkb_geometry) as n_geoms',
				]);
			}

			// split table into N_CORES queries
			let a_queries = split_query({
				rows: n_features,
				table: s_table,
				query: /*syntax: sql */ `
					select *, ${a_select.join(', ')}
						from ${s_table}
						where st_isvalid(wkb_geometry)
					`.replace(/\n/g, ' '),
			});

			// triplify feature table
			await k_group
				.use(a_queries)
				.map('triplify', [{
					// how often each worker should update master thread
					update_interval: N_CORES / 8e-3,  // (about 8 times per second)
				}], {
					// progress event
					progress(i_subset, n_rows) {
						// update progress bar
						y_bar.tick(n_rows);
					},
				})
				.end();
		}))
		// triplify tables serially
		.reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());

	// done with client
	y_client.end();

	// done with workers
	k_group.kill();

	// done
	console.log('done'.green);
};

if(module.parent) {
	module.exports = triplify;
}
else {
	process.argv.slice(2)
		.map(async s => triplify(path.basename(s, '.zip')))
		.reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());
}
