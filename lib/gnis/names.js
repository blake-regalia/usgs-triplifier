// native imports
const fs = require('fs');
const path = require('path');

// third-party modules
const local = require('classer').logger('gnis-names');
const json_stream = require('JSONStream');

// local classes
const triplify = require('./triplify');

const P_OUTPUT_DIR = path.resolve(__dirname, '../../data/output/gnis');
const H_GNIS_FEATURE_LOOKUP = require(`${P_OUTPUT_DIR}/features.json`);

// clean IRIs by replacing non-word characters with underscores
const clean = s => s.replace(/[^\w]/g, '_');

let dsk_names = json_stream.stringifyObject('{\n\t', ',\n\t', '\n}\n');
dsk_names.pipe(fs.createWriteStream(`${P_OUTPUT_DIR}/names.json`));

//
triplify('./data/input/gnis/names.zip', {
	subject: (h) => {
		let s_id = h.feature_id;

		let p_iri = H_GNIS_FEATURE_LOOKUP[s_id];
		if(!p_iri) {
			let s_name = h.feature_name;
			local.warn(`no feature found for id "${s_id}"; "${s_name}"`);

			// iri
			p_iri = `gnisf:Name.${clean(s_name)}.${s_id}`;

			// same iri for name
			dsk_names.write([s_id, p_iri]);
		}

		return p_iri;

		// // official name
		// if(h.feature_name_official) {
		// 	let s_name = h.feature_name;

		// 	// feature name conflict
		// 	if(as_official_feature_names.has(s_name)) {
		// 		// // disambiguate this one
		// 		// local.warn(`feature name conflict "${h.feature_name}" resolved by appending feature id`);

		// 		// mk iri
		// 		let p_iri = `gnisf:${clean(s_name)}.${s_id}`;

		// 		// set official id lookup
		// 		h_official_features[s_id] = p_iri;

		// 		// no need to claim name since it should remain unique with feature id
		// 		return p_iri;
		// 	}
		// 	// no conflict
		// 	else {
		// 		// claim this name
		// 		as_official_feature_names.add(s_name);

		// 		// mk iri
		// 		let p_iri = `gnisf:${clean(s_name)}`;

		// 		// set official id lookup
		// 		h_official_features[s_id] = p_iri;

		// 		// done
		// 		return p_iri;
		// 	}
		// }
		// // alternative name
		// else {
		// 	// return official suffix
		// 	return h_official_features[s_id];
		// }
	},

	fields: {
		feature_id: {
			predicate: 'gnis:featureId',
			object: (s, h) => (!H_GNIS_FEATURE_LOOKUP[s] && h.feature_name_official)? '"'+s: '',
		},
		feature_name(s, h) {
			if(h.feature_name_official) {
				return {
					'rdfs:label': ['@en"'+s],
					'gnis:officialName': ['@en"'+s],
				};
			}
			else {
				return {
					'gnis:alternativeName': ['@en"'+s],
				};
			}
		},
		citation: {
			predicate: 'gnis:citation',
			object: (s, h) => {
				if(h.feature_name_official) {
					if('Citation Unknown' === s) return 'gnis:UnknownCitation';
					else return '@en"'+s;
				}
			},
		},
		date_created: {
			predicate: 'gnis:dateNameCreated',
			object: (s, h) => (s && h.feature_name_official)? new Date(s): '',
			optional: true,
		},
	},
}, () => {
	dsk_names.end();
	local.good('all done');
});
