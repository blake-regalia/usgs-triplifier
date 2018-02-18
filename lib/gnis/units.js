// native imports
const fs = require('fs');

// third-party modules
const local = require('classer').logger('gnis-units');

// local classes
const triplify = require('./triplify');

// states
let h_states = {};

// clean IRIs by replacing non-word characters with underscores
const clean = s => s.replace(/[^\w]/g, '_');

//
triplify('./data/input/gnis/units.zip', {
	// each feature's iri
	subject: (h) => {
		switch(h.unit_type) {
			case 'COUNTY': return `gnisf:${clean(h.state_name)}.${clean(h.county_name)}`;
			case 'STATE': return `gnisf:${clean(h.state_name)}`;
		}

		throw `invalid unit type: "${h.unit_type}"`;
	},

	// map csv fields to triples
	fields: {
		feature_id: {
			predicate: 'gnis:featureId',
			object: s => '"'+s,
		},
		unit_type: {
			predicate: 'a',
			object: s => 'gnis:'+clean(s[0]+s.slice(1).toLowerCase()),
		},
		county_numeric: {
			predicate: 'gnis:countyCode',
			object: s => '"'+s,
			optional: true,
		},
		county_name: {
			predicate: 'gnis:countyName',
			object: s => '@en"'+s,
			optional: true,
		},
		state_numeric: {
			predicate: 'gnis:stateId',
			object: (s, h) => 'STATE' === h.unit_type? '"'+s: '',
		},
		state_alpha: {
			predicate: 'gnis:stateCode',
			object: (s, h) => {
				if('STATE' === h.unit_type) {
					h_states[s] = clean(h.state_name);
					return '"'+s;
				}
			},
		},
		state_name(s, h) {
			if('COUNTY' === h.unit_type) {
				return {
					'gnis:state': ['gnisf:'+clean(s)],
				};
			}
			else {
				return {
					'gnis:stateName': ['@en"'+s],
				};
			}
		},
		country_name: {
			predicate: 'gnis:country',
			object: s => 'gnisf:'+clean(s),
		},
		feature_name: {
			predicate: 'rdfs:label',
			object: s => '@en"'+s,
		},
	},
}, () => {
	let s_json = JSON.stringify(h_states, null, '\t');
	fs.writeFile('./data/output/gnis/states.json', s_json, () => {
		local.good('states.json');
	});

});
