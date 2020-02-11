
const nt_read = require('@graphy/content.nt.read');
const ttl_read = require('@graphy/content.ttl.read');
const ttl_write = require('@graphy/content.ttl.write');
const path = require('path');
const fs = require('fs');

const app_config = require('../../config.app.js');

let ds_dbpedia = fs.createReadStream(path.join(__dirname, '../../data/output/gnis/same-as.ttl'));
let ds_geonames = fs.createReadStream(path.join(__dirname, '../../data/input/dbpedia/same-as-geonames.nt'));

let h_dbp_geonames = {};

nt_read(ds_geonames, {
	data(g_quad) {
		let p_dbp = g_quad.subject.value;
		let p_geonames = g_quad.object.value;

		h_dbp_geonames[p_dbp] = p_geonames;
	},

	eof() {
		console.log('parsed nt file');

		let ds_out = ttl_write({
			prefixes: app_config.prefixes,
		});

		ds_out.pipe(fs.createWriteStream(path.join(__dirname, '../../data/output/gnis/same-as-geonames.nt')));

		ttl_read(ds_dbpedia, {
			data(g_quad) {
				let p_dbp = g_quad.object.value;
				if(p_dbp in h_dbp_geonames) {
					ds_out.write({
						type: 'c3',
						value: {
							[`>${g_quad.subject.value}`]: {
								'owl:sameAs': `>${h_dbp_geonames[p_dbp]}`,
							},
						},
					});
				}
			},

			eof() {
				ds_out.end();
			},
		});
	},
});
