// native imports
const fs = require('fs');
const path = require('path');

// third-party modules
const json_stream = require('JSONStream');

// local classes
const triplify = require('./triplify');

const P_OUTPUT_DIR = path.resolve(__dirname, '../../data/output/gnis');

let dsk_names = json_stream.stringifyObject('{\n\t', ',\n\t', '\n}\n');
dsk_names.pipe(fs.createWriteStream(`${P_OUTPUT_DIR}/names.json`));

//
triplify('./data/input/gnis/names.zip', {
	subject: (h) => {
		return `gnisf:${+h.feature_id}`;
	},

	fields: {
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
	console.log('all done');
});
