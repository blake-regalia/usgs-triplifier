// native imports
const fs = require('fs');
const path = require('path');

// third-party modules
const json_stream = require('JSONStream');

// local classes
const triplify = require('./triplify');

// local data
const app_config = require('../../config.app.js');

// iri for geometry
const P_GEOM_URI = `${app_config.data_uri}/geometry`;

// prep output dir
const P_OUTPUT_DIR = path.resolve(__dirname, '../../data/output/gnis');

// import lookups
const H_GNIS_STATE_LOOKUP = require(`${P_OUTPUT_DIR}/states.json`);
// const H_GNIS_NAME_LOOKUP = require(`${P_OUTPUT_DIR}/names.json`);

// setup
const H_GNIS_CLASS_MAPPINGS = {
	Range: 'MountainRange',
	Falls: 'Waterfall',
	Woods: 'Woodland',
	Oilfield: 'OilField',
	Civil: 'CivilGovernment',
	Pillar: 'Rock',
};

// simplest uris claimed by features
let as_feature_uris = new Set();

// clean IRIs by replacing non-word characters with underscores
const clean = s => s.replace(/[^\w]/g, '_');

// geometry output
let ds_geoms = fs.createWriteStream(`${P_OUTPUT_DIR}/geoms.tsv`);
let dsk_features = json_stream.stringifyObject('{\n\t', ',\n\t', '\n}\n');
dsk_features.pipe(fs.createWriteStream(`${P_OUTPUT_DIR}/features.json`));

//
triplify('./data/input/gnis/features.zip', {
	// each feature's iri
	subject: (h) => {
		// ref feature id
		let s_id = h.feature_id;

		// fetch class name
		let s_class = h.feature_class.replace(/ /g, '');
		if(s_class in H_GNIS_CLASS_MAPPINGS) s_class = H_GNIS_CLASS_MAPPINGS[s_class];

		// fetch state name
		let s_state = H_GNIS_STATE_LOOKUP[h.state_alpha];

		// mk feature uri
		let s_name = h.feature_name;
		let s_uid = `${h.state_alpha}${h.county_numeric}.${s_class}.${s_name}`;
		let p_iri = `gnisf:${s_state}.${clean(h.county_name)}.${s_class}.${clean(s_name)}`;

		// feature uri conflict
		if(as_feature_uris.has(s_uid)) {
			// local.warn(`resolving feature uri conflict for "${p_iri}"`);

			// resolve conflict
			p_iri += `.${s_id}`;
		}
		// no conflict
		else {
			// claim simple uri
			as_feature_uris.add(s_uid);
		}

		// store name association
		dsk_features.write([s_id, p_iri]);

		// save association(s) to object
		h.class = s_class;
		h.uri = p_iri;

		//
		return p_iri;
	},

	// map csv fields to triples
	fields: {
		feature_id: {
			predicate: 'gnis:featureId',
			object: s => '^xsd:integer"'+s,
		},
		// feature_name: {
		// 	predicate: 'rdfs:label',
		// 	object: s => '@en"'+s,
		// },
		feature_class: {
			predicate: 'rdf:type',
			object: (s) => {
				s = s.replace(/ /g, '');
				if(s in H_GNIS_CLASS_MAPPINGS) s = H_GNIS_CLASS_MAPPINGS[s];
				return 'cegis:'+s;
			},
		},
		state_alpha: {
			predicate: 'gnis:state',
			object: s => 'gnisf:'+H_GNIS_STATE_LOOKUP[s],
		},
		county_name: {
			predicate: 'gnis:county',
			object: (s, h) => 'gnisf:'+(s? H_GNIS_STATE_LOOKUP[h.state_alpha]+`.${clean(s)}`: 'gnis:UnknownCounty'),
			optional: true,
		},

		// PRIMARY_LAT_DMS|PRIM_LONG_DMS
		// PRIM_LAT_DEC|PRIM_LONG_DEC
		// SOURCE_LAT_DMS|SOURCE_LONG_DMS
		// SOURCE_LAT_DEC|SOURCE_LONG_DEC
		prim_lat_dec(s, h, k) {
			if(s) {
				let s_lat = s;
				let s_lng = h.prim_long_dec;
				let p_geom_iri = `${P_GEOM_URI}/point/gnisf.${h.feature_id}`;
				let s_point_wkt = `POINT(${s_lng} ${s_lat})`;
				ds_geoms.write(`${p_geom_iri}\tSRID=4326;${s_point_wkt}\t${h.feature_name}\t${h.uri}\t${h.class}\n`);

				// add wkt literal to geometry node
				k.add({
					[`>${p_geom_iri}`]: {
						'geosparql:asWKT': [`^geosparql:wktLiteral"<http://www.opengis.net/def/crs/OGC/1.3/CRS84>${s_point_wkt}`],
					},
				});

				// geometry uri
				return {
					'ago:geometry': [`>${p_geom_iri}`],
				};
			}
		},
		// elev_in_m: {
		// 	predicate: 'gnis:elevation',
		// 	object: (s) => ({
		// 		'qudt:numericValue': '^xsd:double"'+s,
		// 		'qudt:unit': 'unit:M',
		// 	}),
		// 	optional: true,
		// },
		elev_in_ft: {
			predicate: 'gnis:elevation',
			object: (s, h, k) => {
				let p_elevation = 'gnis:Elevation.'+s+'ft';
				k.add({
					[p_elevation]: {
						'qudt:numericValue': '^xsd:double"'+s,
						'qudt:unit': 'unit:FT',
					},
				});
				return p_elevation;
			},
			optional: true,
		},
		map_name: {
			predicate: 'gnis:mapName',
			object: s => '@en"'+s,
		},
		date_created: {
			predicate: 'gnis:dateFeatureCreated',
			object: s => s? new Date(s): '',
			optional: true,
		},
		date_edited: {
			predicate: 'gnis:dateFeatureEdited',
			object: s => s? new Date(s): '',
			optional: true,
		},
	},
}, () => {
	dsk_features.end();
	ds_geoms.end();
});
