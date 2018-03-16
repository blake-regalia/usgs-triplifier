
const graphy = require('graphy');
const path = require('path');
const fs = require('fs');

let ds_dbpedia = fs.createReadStream(path.join(__dirname, '../../data/output/gnis/same-as.ttl'));
let ds_geonames = fs.createReadStream(path.join(__dirname, '../../data/input/dbpedia/same-as-geonames.nt'));

let h_dbp_geonames = {};

graphy.nt.deserializer(ds_geonames, {
	data(h_triple) {
		let p_dbp = h_triple.subject.value;
		let p_geonames = h_triple.object.value;

		h_dbp_geonames[p_dbp] = p_geonames;
	},

	end() {
		console.log('parsed nt file');
		let ds_out = fs.createWriteStream(path.join(__dirname, '../../data/output/gnis/same-as-geonames.nt'));

		graphy.ttl.deserializer(ds_dbpedia, {
			data(h_triple) {
				let p_dbp = h_triple.object.value;
				if(p_dbp in h_dbp_geonames) {
					ds_out.write(`<${h_triple.subject.value}> <http://www.w3.org/2002/07/owl#sameAs> <${h_dbp_geonames[p_dbp]}>\n`);
				}
			},

			end() {
				ds_out.end();
			},
		});
	},
});
