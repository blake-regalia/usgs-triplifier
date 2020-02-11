// native imports
const fs = require('fs');
const path = require('path');

// third-party modules
const ttl_write = require('@graphy/content.ttl.write');

// local classes
const triplify = require('./triplify');

// local data
const app_config = require('../../config.app.js');

// iri for geometry
const P_GEOM_URI = app_config.geom_uri;

// prep output dir
const P_OUTPUT_DIR = path.resolve(__dirname, '../../data/output/gnis');

// import lookups
const H_GNIS_STATE_LOOKUP = require(`${P_OUTPUT_DIR}/states.json`);

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
let h_feature_aliases = {};

// clean IRIs by replacing non-word characters with underscores
const clean = s => s.replace(/[^\w]/g, '_');

// geometry output
let ds_geoms = fs.createWriteStream(`${P_OUTPUT_DIR}/geoms.tsv`);

//
triplify('./data/input/gnis/features.zip', {
	// each feature's iri
	subject: (h) => {
		// ref feature id
		let n_id = h.feature_id.replace(/^0+/, '') || '0';

		// fetch class name
		let s_class = h.feature_class.replace(/ /g, '');
		if(s_class in H_GNIS_CLASS_MAPPINGS) s_class = H_GNIS_CLASS_MAPPINGS[s_class];

		// fetch state name
		let s_state = H_GNIS_STATE_LOOKUP[h.state_alpha];

		// mk feature uri
		let s_name = h.feature_name;

		// feature alias
		let si_alias = `${s_state}.${clean(h.county_name)}.${s_class}.${clean(s_name)}`;

		// alias exists
		if(si_alias in h_feature_aliases) {
			// add to list
			h_feature_aliases[si_alias].push(n_id);
		}
		// first encounter of alias
		else {
			// save to hash
			h_feature_aliases[si_alias] = [n_id];
		}

		// save association(s) to object
		h.class = s_class;
		// h.uri = p_iri;

		// actual subject iri
		return `gnisf:${n_id}`;
	},

	// map csv fields to triples
	fields: {
		feature_id: {
			predicate: 'gnis:featureId',
			object: s => '"'+s,
		},
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
			object: s => 'gnisf-alias:'+clean(s),
		},
		county_name: {
			predicate: 'gnis:county',
			object: (s, h) => 'gnisf-alias:'+(s? H_GNIS_STATE_LOOKUP[h.state_alpha]+`.${clean(s)}`: 'gnis:UnknownCounty'),
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
				ds_geoms.write(`${p_geom_iri}\tSRID=4326;${s_point_wkt}\n`);

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
		elev_in_ft(s, h, k) {
			if(s) {
				let sc1_elevation = `gnis:Elevation.${s}ft`;

				// add elevation info to elevationnode
				k.add({
					[sc1_elevation]: {
						'qudt:numericValue': '^xsd:double"'+s,
						'qudt:unit': 'unit:FT',
					},
				});

				// elevation uri
				return {
					'gnis:elevation': sc1_elevation,
				};
			}
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
	// close geometry output stream
	ds_geoms.end();

	// serialize aliases
	let ds_aliases = ttl_write({
		prefixes: app_config.prefixes,
	});

	// pipe output to file
	ds_aliases.pipe(fs.createWriteStream(`${P_OUTPUT_DIR}/feature-aliases.ttl`));

	// each alias
	for(let p_alias in h_feature_aliases) {
		let a_features = h_feature_aliases[p_alias];

		// unique
		if(1 === a_features.length) {
			// add same-as to alis
			ds_aliases.write({
				type: 'c3',
				value: {
					[`gnisf-alias:${p_alias}`]: {
						'owl:sameAs': `gnisf:${a_features[0]}`,
					},
				},
			});
		}
		// not unique
		else {
			// add disambiguation
			ds_aliases.write({
				type: 'c3',
				value: {
					[`gnisf-alias:${p_alias}`]: {
						'gnis:disambiguatesTo': a_features.map(s => `gnisf:${s}`),
					},
				},
			});
		}
	}

	// end stream
	ds_aliases.end();
});
