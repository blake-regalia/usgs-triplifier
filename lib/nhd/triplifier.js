
const fs = require('fs');

const graphy = require('graphy');
const worker = require('worker');
const pg = require('pg');
const pg_cursor = require('pg-cursor');

const app_config = require('../../config.app.js');
const P_DATA_URI = app_config.data_uri;
const P_GEOM_URI = `${P_DATA_URI}/geometry`;

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


// over the lifespan of this worker, we will only need a pool of one worker
let y_pool = new pg.Pool({max:1});

let H_STATIC = {};

const H_TABLES_STATIC = {
	nhdfcode: {
		rename: 'fcode',
		key: 'fcode',
	},
};



const wkt_to_ct = s => `^geosparql:wktLiteral"<http://www.opengis.net/def/crs/EPSG/0/4326>${s}`;

const qty_to_wo = (s_value, s_symbol) => ({
	'qudt:numericValue': graphy.double(s_value),
	'qudt:unit': 'unit:'+s_symbol,
});

class Triplifier {
	constructor(s_table, i_subset, h_static) {
		// output files
		let p_triples = `./${s_table}_${i_subset}.ttl`;

		// create serializer
		let k_writer = graphy.ttl.writer({
			prefixes: app_config.prefixes,
			coercions: new Map([
				[Date, (dt) => graphy.date(dt)],
				[Number, (x) => graphy.number(x)],
			]),
		});

		// pipe output to file
		k_writer.pipe(fs.createWriteStream(p_triples));

		// fields
		Object.assign(this, {
			table: s_table,
			static: h_static,
			writer: k_writer,
		});
	}

	close() {
		// end writer and close output file
		this.writer.end();
	}
}


class Triplifier_Feature extends Triplifier {
	constructor(s_table, i_subset, ...a_args) {
		super(s_table, i_subset, ...a_args);

		// geometry output
		let p_geoms = `./${s_table}_${i_subset}.tsv`;

		// geometry stream
		let ds_geoms = fs.createWriteStream(p_geoms);

		// geometry type
		let hpg_type = H_TABLES_FEATURES[s_table];

		// fields
		Object.assign(this, {
			geometry_type: hpg_type,
			geometry_stream: ds_geoms,
		});
	}

	close() {
		super.close();

		// close geometry stream
		this.geometry_stream.end();
	}

	async row(h_row) {
		let {
			table: s_table,
			writer: k_writer,
			geometry_type: hpg_type,
			geometry_stream: ds_geoms,
		} = this;

		// debugger;
		let si_feature = h_row.permanent_identifier;

		// feature iri; save association to hash
		let sct_self = id_to_ct(si_feature);

		// fetch ftype id
		let sct_ftype = this.static.fcode[h_row.fcode || H_CODES.fcode_defaults[s_table]];

		// pairs hash
		let h_pairs = {
			// permanent identifier
			'usgs:permanentIdentifier': '"'+si_feature,

			// feature type
			'rdf:type': sct_ftype || 'nhd:Default_FCode_'+s_table,

			// date of last modification
			'usgs:modified': new Date(h_row.fdate),

			// resolution
			'nhd:resolution': code_to_ct('resolution', h_row.resolution),
		};

		// triple writer
		let h_triples = {
			[sct_self]: h_pairs,
		};

		// gnis id
		if(h_row.gnis_id) {
			h_pairs['nhd:gnisFeature'] = `gnisf:${h_row.gnis_id.replace(/^0+/, '') || '0'}`;
		}

		// polyline
		if(hpg_type.linear) {
			// length
			if('lengthkm' in h_row && null !== h_row.lengthkm) {
				h_pairs['nhd:length'] = qty_to_wo(h_row.lengthkm, 'KM');
			}
		}
		// polygon
		else if(hpg_type.areal) {
			// area
			if('areasqkm' in h_row && null !== h_row.areasqkm) {
				h_pairs['nhd:area'] = qty_to_wo(h_row.areasqkm, 'KM2');
			}
		}

		// elevation
		if('elevation' in h_row && null !== h_row.elevation) h_pairs['nhd:elevation'] = qty_to_wo(h_row.elevation, 'M');

		// reach code
		if('reachcode' in h_row && null !== h_row.reachcode) h_pairs['nhd:reachCode'] = '"'+h_row.reachcode;

		// flow direction
		if('flowdir' in h_row) h_pairs['nhd:hydroFlowDirection'] = code_to_ct('hydro_flow_direction', h_row.flowdir);

		// concise term string
		let sct_geometry = `usgeo-${hpg_type.lower}:${sct_self.replace(':', '.')}`;

		// store geometry to feature
		h_pairs['ago:geometry'] = sct_geometry;

		// add triples about geometry
		let h_geom = h_triples[sct_geometry] = {
			'rdf:type': 'ago:'+hpg_type.proper,
		};

		// multi-part geometry; add count
		if(hpg_type.multipart) {
			h_geom['ago:geometryCount'] = graphy.integer(h_row.n_geoms);
		}

		// yes a point
		if(HPG_POINT === hpg_type) {
			h_geom['geosparql:asWKT'] = wkt_to_ct(h_row.wkt_geometry);
		}
		// not a point
		else {
			Object.assign(h_geom, {
				// add centroid
				'ago:centroid': wkt_to_ct(h_row.wkt_centroid),

				// add bounding box
				'ago:boundingBox': wkt_to_ct(h_row.wkt_bounding_box),

				// point count
				'ago:pointCount': graphy.integer(h_row.n_points),
			});
		}

		// linear
		if(hpg_type.linear) {
			h_geom['ago:length'] = qty_to_wo(h_row.x_length, 'M');
		}

		// areal
		if(hpg_type.areal) {
			Object.assign(h_geom, {
				// area
				'ago:area': qty_to_wo(h_row.x_area, 'M2'),

				// perimeter
				'ago:perimeter': qty_to_wo(h_row.x_perimeter, 'M'),

				// number of rings
				'ago:ringCount': graphy.integer(h_row.n_rings),
			});
		}

		// endpoints
		if(h_row.wkt_start_point) {
			h_geom['ago:endpoint'] = [
				wkt_to_ct(h_row.wkt_start_point),
				wkt_to_ct(h_row.wkt_end_point),
			];
		}

		// output triples
		k_writer.add(h_triples);

		// serialize geometry
		await new Promise((fk_row) => {
			ds_geoms.write([
				`${P_GEOM_URI}/${hpg_type.lower}/${sct_self.replace(':', '.')}`,  // full iri
				`${h_row.wkb_geometry}`,
			].join('\t')+'\n', () => {
				fk_row();
			});
		});
	}
}

class Triplifier_Flow extends Triplifier {
	async row(h_row) {
		let h_pairs = {};

		// fetch flow direction semantic label
		let s_flow_direction = code_to_ct('flow_direction', h_row.direction);

		// normal direction
		if(s_flow_direction.includes(':In_')) {
			h_pairs['nhd:flowsInto'] = id_to_ct(h_row.to_permanent_identifier);
		}
		// network start
		else if(s_flow_direction.includes(':Network_Start_')) {
			h_pairs['nhd:flowPosition'] = s_flow_direction;
		}
		// network end
		else if(s_flow_direction.includes(':Network_End_')) {
			h_pairs['nhd:flowPosition'] = s_flow_direction;
			h_pairs['nhd:flowsInto'] = 'rdf:nil';
		}

		// output triples
		this.writer.add({
			[id_to_ct(h_row.from_permanent_identifier)]: h_pairs,
		});
	}
}


const H_TABLES_LOCAL = {
	nhdflow: Triplifier_Flow,
};

worker.dedicated({
	async triplify(h_subset, h_config={}) {
		let {
			index: i_subset,
			rows: n_rows,
			table: s_table,
			query: s_query,
		} = h_subset;

		let {
			update_interval: t_interval=1000,
		} = h_config;

		let k_self = this;

		// triplifier
		let k_triplifier = (() => {
			if(s_table in H_TABLES_FEATURES) {
				return new Triplifier_Feature(s_table, i_subset, H_STATIC);
			}
			else if(s_table in H_TABLES_LOCAL) {
				return new H_TABLES_LOCAL[s_table](s_table, i_subset);
			}
		})();

		// checkout a client
		let y_client = await y_pool.connect();

		// create cursor on range
		let y_cursor = y_client.query(new pg_cursor(s_query));

		// chunk size (initially)
		let n_chunk_size = 8;

		// next cursor read
		await new Promise((fk_consume) => {
			(function f_read() {
				// start timing
				let t_start = Date.now();

				// read from cursor
				y_cursor.read(n_chunk_size, async (e_read, a_rows) => {
					if(e_read) {
						debugger;
						s_query;
						throw e_read;
					}

					// each row serially
					await a_rows
						.map(h_row => async () => {
							try {
								await k_triplifier.row(h_row);
							}
							catch(e_f) {
								debugger;
								throw e_f;
							}
						})

						// thru async each
						.reduce((dp_a, dp_b) => dp_a.then(dp_b), Promise.resolve());

					// stop timing
					let t_elapsed = Date.now() - t_start;

					// progress
					k_self.emit('progress', a_rows.length);

					// more to consume; read more
					if(n_chunk_size === a_rows.length) {
						// interval adjustment scalar
						let xs_adjust = t_interval / t_elapsed;

						// adjust chunk size
						n_chunk_size = Math.ceil(n_chunk_size * xs_adjust) || 16;

						if(isNaN(n_chunk_size) || n_chunk_size < 1 || n_chunk_size === Infinity) {
							debugger;
							t_elapsed;
							t_start;
							t_interval;
							xs_adjust;
						}

						// repeat
						setImmediate(f_read);
					}
					// all done; complete promise
					else {
						fk_consume();
					}
				});
			})();
		});

		// release client back to pool
		y_client.release();

		// close triplifier
		k_triplifier.close();
	},

	async init(h_static) {
		H_STATIC = h_static;
	// 	// load each in parallel
	// 	await Promise.all(Object.keys(H_TABLES_STATIC)
	// 		.map(s_table => (async () => {
	// 			// fetch rows
	// 			let a_rows = (await y_pool.query(`select * from ${s_table}`)).rows;

	// 			// attribute info
	// 			let {
	// 				rename: s_rename,
	// 				key: s_key,
	// 				cardinality: s_cardinality,
	// 			} = H_TABLES_STATIC[s_table];

	// 			// make hash
	// 			let h_hash = H_STATIC[s_rename] = {};

	// 			// make each key a list
	// 			if('many' === s_cardinality) {
	// 				// save to hash
	// 				a_rows.forEach((h_row) => {
	// 					let s_group = h_row[s_key];

	// 					// make list
	// 					if(!(s_group in h_hash)) {
	// 						h_hash[s_group] = [h_row];
	// 					}
	// 					// push item to list
	// 					else {
	// 						h_hash[s_group].push(h_row);
	// 					}
	// 				});
	// 			}
	// 			// singletons
	// 			else {
	// 				// save to hash
	// 				a_rows.forEach((h_row) => {
	// 					// under given key
	// 					h_hash[h_row[s_key]] = h_row;
	// 				});
	// 			}
	// 		})()));
	},
});
