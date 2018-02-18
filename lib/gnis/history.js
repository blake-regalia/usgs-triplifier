// native imports
const path = require('path');

// third-party modules
const local = require('classer').logger('gnis-history');

// local classes
const triplify = require('./triplify');

const P_OUTPUT_DIR = path.resolve(__dirname, '../../data/output/gnis');
const H_GNIS_FEATURE_LOOKUP = require(`${P_OUTPUT_DIR}/features.json`);
const H_GNIS_NAME_LOOKUP = require(`${P_OUTPUT_DIR}/names.json`);

// // clean IRIs by replacing non-word characters with underscores
// const clean = s => s.replace(/[^\w]/g, '_');

//
triplify('./data/input/gnis/history.zip', {
	subject: (h) => {
		let s_id = h.feature_id;

		let p_iri = H_GNIS_FEATURE_LOOKUP[s_id];
		if(!p_iri) {
			p_iri = H_GNIS_NAME_LOOKUP[s_id];
			if(!p_iri) {
				local.error(`no feature/name found for ${s_id}`);
			}
			else {
				local.warn(`no feature found for id "${s_id}";`);
			}
		}

		return p_iri;
	},

	fields: {
		description: {
			predicate: 'gnis:description',
			object: s => '@en"'+s,
			optional: true,
		},
		history: {
			predicate: 'gnis:history',
			object: s => '@en"'+s,
			optional: true,
		},
	},
}, () => {
	local.good('all done');
});
