
// native imports
const child_process = require('child_process');
const fs = require('fs');
const path = require('path');

// third-party includes
const async = require('async');
const classer = require('classer');
const mkdirp = require('mkdirp');
const pg = require('pg');
const graphy = require('graphy');

// local classes
const psql_config = require('../marmotta/psql-config.js');

const app_config = require('../../config.app.js');
const P_DATA_URI = app_config.data_uri;
const P_GEOM_URI = `${P_DATA_URI}/geometry`;

//
const local = classer.logger('gdb_ttl');

let s_loader_db = 'usgs';
let y_pool = new pg.Pool(Object.assign(psql_config, {
	database: s_loader_db,
}));


const S_TT_WKT_LITERAL = '^geosparql:wktLiteral"<http://www.opengis.net/def/crs/OGC/1.3/CRS84>';

const R_SINGLE_MULTIPOINT = /^MULTIPOINT\(([^,]+)\)$/;

const R_POINT = /^POINT\(\s*(.+[^\s])\s*\)$/;
const R_MULTIPOINT = /^MULTIPOINT\(/;
const R_LINESTRING = /^LINESTRING\(\s*(.+[^\s])\s*\)$/;
const R_MULTILINESTRING = /^MULTILINESTRING\(\s*\(\s*(.+[^\s])\s*\)\s*\)$/;
const R_POLYGON = /^POLYGON\(\s*\(\s*(.+[^\s])\s*\)\s*\)$/;
const R_MULTIPOLYGON = /^MULTIPOLYGON\(\s*\(\s*\(\s*(.+[^\s])\s*\)\s*\)\s*\)$/;


const P_OUTPUT_DIR = path.resolve(__dirname, '../../data/output');
const H_GNIS_FEATURE_LOOKUP = require(`${P_OUTPUT_DIR}/gnis/features.json`);

let p_loader_url = `postgres://${psql_config.user}:${psql_config.password}@localhost${psql_config.port? ':'+psql_config.port: ''}/${s_loader_db}`;
child_process.execSync(`psql ${psql_config.url} -c "drop database ${s_loader_db};"`);
child_process.execSync(`psql ${psql_config.url} -c "create database ${s_loader_db};"`);
child_process.execSync(`psql -d ${p_loader_url} -c "create extension postgis;"`);

y_pool.connect(function(e_connect, y_client, fk_client) {
	if(e_connect) {
		console.dir(psql_config);
		local.fail(`could not connect to ${s_loader_db} database: `+e_connect);
	}

	// 
	const extract_table = function(s_sql_dir, s_table_name, f_okay_extract) {
		// extract wkt geometry from table (force multi)
		y_client.query(`
			select *, ST_AsText(ST_Multi(ST_Force_2D(geom))) as wkt
			from "${s_table_name}"
		`, function(e_query, h_result) {
			let p_output_dir = `./data/output/geodatabases/${s_sql_dir}`; 
			mkdirp.sync(p_output_dir);

			if(e_query) {
				local.fail('query error: '+e_query);
			}

			let p_geoms = `${p_output_dir}/${s_table_name}.tsv`;
			let p_triples = `${p_output_dir}/${s_table_name}.ttl`;
			let ds_geoms = fs.createWriteStream(p_geoms);
			// let ds_triples = fs.createWriteStream(p_triples);

			// create serializer
			let h_xsd_date = graphy.namedNode('http://www.w3.org/2001/XMLSchema#date');
			let h_xsd_integer = graphy.namedNode('http://www.w3.org/2001/XMLSchema#integer');
			let k_serializer = graphy.ttl.serializer({
				prefixes: app_config.prefixes,
				coercions: new Map([
					[Date, (dt) => graphy.literal(dt.toISOString().replace(/T.+$/, ''), h_xsd_date)],
					[Number, (x) => graphy.literal(x+'', h_xsd_integer)],
				]),
			});

			// pipe output to file
			k_serializer.pipe(fs.createWriteStream(p_triples));

			// fetch writer
			let k_writer = k_serializer.writer;

			let c_features = 0;
			h_result.rows.forEach((h_row) => {
				// only write permanent features
				if(h_row.permanent_) {
					let s_permament_id = h_row.permanent_.replace(/([{}])/g, (s) => encodeURIComponent(s));
					let s_subject = `cegisf:${s_permament_id}`;

					// sanitize wkt
					let s_wkt = h_row.wkt;
					let m_multipoint = R_SINGLE_MULTIPOINT.exec(s_wkt);
					if(m_multipoint) {
						s_wkt = 'POINT('+m_multipoint[1]+')';
					}

					//
					let s_type = '';
					let h_bounds = {};
					let m_point = R_POINT.exec(s_wkt);
					if(m_point) {
						s_type = 'point';
						let [s_lng, s_lat] = s_wkt.split(/\s+/);
						h_bounds = {
							point: {lng:s_lng, lat:s_lat},
						};
					}
					else {
						let m_polygon = R_POLYGON.exec(s_wkt);
						if(m_polygon) {
							s_type = 'polygon';
							let x_lng_min=Infinity, x_lat_min=Infinity, x_lng_max=-Infinity, x_lat_max=-Infinity;
							m_polygon[1].split(/\s*\)\s*,\s*\(\s*/g).forEach((s_ring) => {
								s_ring.split(/\s*,\s*/g).forEach((s_crd) => {
									let [s_lng, s_lat] = s_crd.split(/\s+/);
									let x_lng = +s_lng;
									let x_lat = +s_lat;
									x_lng_min = Math.min(x_lng_min, x_lng);
									x_lat_min = Math.min(x_lat_min, x_lat);
									x_lng_max = Math.max(x_lng_max, x_lng);
									x_lat_max = Math.max(x_lat_max, x_lat);
								});
							});

							h_bounds = {
								box: {
									south: x_lat_min,
									north: x_lat_max,
									west: x_lng_min,
									east: x_lng_max,
								},
							};
						}
						else {
							let m_linestring = R_LINESTRING.exec(s_wkt);
							if(m_linestring) {
								s_type = 'linestring';
								let x_lng_min=Infinity, x_lat_min=Infinity, x_lng_max=-Infinity, x_lat_max=-Infinity;
								m_linestring[1].split(/\s*,\s*/g).forEach((s_crd) => {
									let [s_lng, s_lat] = s_crd.split(/\s+/);
									let x_lng = +s_lng;
									let x_lat = +s_lat;
									x_lng_min = Math.min(x_lng_min, x_lng);
									x_lat_min = Math.min(x_lat_min, x_lat);
									x_lng_max = Math.max(x_lng_max, x_lng);
									x_lat_max = Math.max(x_lat_max, x_lat);
								});


								h_bounds = {
									box: {
										south: x_lat_min,
										north: x_lat_max,
										west: x_lng_min,
										east: x_lng_max,
									},
								};
							}
							else {
								let m_multipolygon = R_MULTIPOLYGON.exec(s_wkt);
								if(m_multipolygon) {
									s_type = 'multipolygon';
									let x_lng_min=Infinity, x_lat_min=Infinity, x_lng_max=-Infinity, x_lat_max=-Infinity;
									m_multipolygon[1].split(/\s*\)\s*\)\s*,\s*\(\s*\(\s*/g).forEach((s_polygon) => {
										s_polygon.split(/\s*\)\s*,\s*\(\s*/g).forEach((s_ring) => {
											s_ring.split(/\s*,\s*/g).forEach((s_crd) => {
												let [s_lng, s_lat] = s_crd.split(/\s+/);
												let x_lng = +s_lng;
												let x_lat = +s_lat;
												x_lng_min = Math.min(x_lng_min, x_lng);
												x_lat_min = Math.min(x_lat_min, x_lat);
												x_lng_max = Math.max(x_lng_max, x_lng);
												x_lat_max = Math.max(x_lat_max, x_lat);
											});
										});
									});

									h_bounds = {
										box: {
											south: x_lat_min,
											north: x_lat_max,
											west: x_lng_min,
											east: x_lng_max,
										},
									};
								}
								else {
									let m_multilinestring = R_MULTILINESTRING.exec(s_wkt);
									if(m_multilinestring) {
										s_type = 'multilinestring';

										let x_lng_min=Infinity, x_lat_min=Infinity, x_lng_max=-Infinity, x_lat_max=-Infinity;
										m_multilinestring[1].split(/\s*\)\s*,\s*\(\s*/g).forEach((s_linestring) => {
											s_linestring.split(/\s*,\s*/g).forEach((s_crd) => {
												let [s_lng, s_lat] = s_crd.split(/\s+/);
												let x_lng = +s_lng;
												let x_lat = +s_lat;
												x_lng_min = Math.min(x_lng_min, x_lng);
												x_lat_min = Math.min(x_lat_min, x_lat);
												x_lng_max = Math.max(x_lng_max, x_lng);
												x_lat_max = Math.max(x_lat_max, x_lat);
											});
										});

										h_bounds = {
											box: {
												south: x_lat_min,
												north: x_lat_max,
												west: x_lng_min,
												east: x_lng_max,
											},
										};
									}
									else {
									// let m_multipoint = R_MULTIPOINT.exec(s_wkt);
									// if(m_multipoint) {
									// 	s_type = 'multipoint';
									// }
									// else if(R_MULTILINESTRING.test(s_wkt)) s_type = 'multilinestring';
									// else {
										local.fail(`unknown well known text type: "${s_wkt}"`);
									// }
									}
								}
							}
						}
					}

					// push ttl to output
					let h_pairs = {
						'cegis:permanentId': ['"'+h_row.permanent_],
					};

					// gnis feature
					if(h_row.gnis_id) {
						let s_tt_gnis_feature = H_GNIS_FEATURE_LOOKUP[+h_row.gnis_id];
						if(s_tt_gnis_feature) h_pairs['cegis:fragmentOf'] = [s_tt_gnis_feature];
					}

					// ftype
					if('undefined' !== typeof h_row.ftype) {
						h_pairs['cegis:ftype'] = ['^xsd:integer"'+(+h_row.ftype)];
					}

					// fcode
					if('undefined' !== typeof h_row.ftype) {
						h_pairs['cegis:fcode'] = ['^xsd:integer"'+(+h_row.fcode)];
					}

					// flow direction
					if(h_row.hasOwnProperty('flowdir')) {
						h_pairs['cegis:flowDirection'] = ['^xsd:integer"'+(+h_row.flowdir)];
					}

					// shape length
					if(h_row.hasOwnProperty('shape_leng')) {
						h_pairs['cegis:shapeLength'] = ['^xsd:double"'+(+h_row.shape_leng)];
					}

					// geometry
					let s_tt_geometry = `usgeo-${s_type}:cegisf.${s_permament_id}`;
					let p_geometry_uri = `${P_GEOM_URI}/${s_type}/cegisf.${s_permament_id}`;
					h_pairs['ago:geometry'] = [s_tt_geometry];

					if(h_bounds.box) {
						let {south:x_south, north:x_north, west:x_west, east:x_east} = h_bounds.box;
						k_writer.add({
							[s_tt_geometry]: {
								'ago:boundingBox': [`${S_TT_WKT_LITERAL}POLYGON((${x_west} ${x_north},${x_west} ${x_south},${x_east} ${x_south},${x_east} ${x_north},${x_west} ${x_north}))`],
							},
						});
					}
					else {
						let {lat:x_lat, lng:x_lng} = h_bounds.point;
						k_writer.add({
							[s_tt_geometry]: {
								'geosparql:asWKT': [`${S_TT_WKT_LITERAL}POINT(${x_lng} ${x_lat})`],
							},
						});
					}

					// output to ttl
					k_writer.add({
						[s_subject]: h_pairs,
					});

					// ds_triples.write(`${s_subject}
					// 	cegis:permanentId ${+h_row.permanent_} ;
					// 	${h_row.gnis_id? `cegis:fragmentOf ${H_GNIS_FEATURE_LOOKUP[+h_row.gnis_id]} ; `: ''}
					// 	${'undefined' !== typeof h_row.ftype? `cegis:ftype ${+h_row.ftype} ; `: ''}
					// 	${'undefined' !== typeof h_row.fcode? `cegis:fcode ${+h_row.fcode} ; `: ''}
					// 	${h_row.hasOwnProperty('flowdir')? `cegis:flowDirection ${h_row.flowdir} ; `: ''}
					// 	${h_row.shape_leng? `cegis:shapeLength ${h_row.shape_leng} ; `: ''}
					// `
					// +`	ago:geometry ${s_geometry_terse} ;
					// 	${s_bounding_box_wkt? `ago:boundingBox "<http://www.opengis.net/def/crs/OGC/1.3/CRS84>${s_bounding_box_wkt}"^^geosparql:wktLiteral`
					// 		: `geosparql:hasGeometry "<http://www.opengis.net/def/crs/OGC/1.3/CRS84>${s_point_wkt}"^^geosparql:wktLiteral`}
					// .`.replace(/\n\t+/g, '\n\t')+'\n');

					//
					ds_geoms.write(`${p_geometry_uri}\tSRID=4326;${s_wkt}\n`);

					//gnis:name "${h_row.gnis_name? h_row.gnis_name.replace(/"/g, '\\"'): ""}"@en ;

					// count as feature
					c_features += 1;
				}
			});

			//
			if(c_features) {
				// end writer output
				k_serializer.close();

				// close geometry output stream
				ds_geoms.end();

				local.good('wrote '+c_features+' features to '+p_output_dir);
			}
			// no features
			else {
				// close output streams
				ds_geoms.end(() => {
					k_serializer.on('end', () => {
						fs.unlinkSync(p_geoms);
						fs.unlinkSync(p_triples);
					});

					k_serializer.close();
				});
			}

			f_okay_extract();
		});
	};

	// each input arg (sql directory)
	async.eachSeries(process.argv.slice(2), (s_sql_dir, fk_dir) => {
		// each sql file in directory
		async.eachSeries(fs.readdirSync(s_sql_dir), (s_sql_file, fk_file) => {
			// open sql file
			let df_sql = fs.openSync(`${s_sql_dir}/${s_sql_file}`, 'r');

			// psql process
			let u_psql = child_process.spawn('psql', ['-d', s_loader_db], {
				stdio: [
					df_sql,  // pipe sql file to child's stdin
					'pipe',
					'pipe',
				],
			});

			// once psql process is finished
			u_psql.on('exit', () => {
				// extract geometry data
				extract_table(path.basename(s_sql_dir, '.sql'), s_sql_file.replace(/\.sql$/, '').toLowerCase(), () => {
					fk_file();
				});
			});
		}, () => {
			// all done with directory
			fk_dir();
		});
	}, () => {
		// release postgres client
		fk_client();
		y_client.end();
		process.exit(0);
	});
});

