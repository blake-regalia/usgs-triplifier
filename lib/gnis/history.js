
// local classes
const triplify = require('./triplify');

//
triplify('./data/input/gnis/history.zip', {
	subject: (h) => {
		return `gnisf:${h.feature_id}`;
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
	console.log('all done');
});
