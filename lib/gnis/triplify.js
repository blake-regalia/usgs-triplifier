
// native imports
const fs = require('fs');
const path = require('path');
const util = require('util');

// third-party includes
const local = require('classer').logger('triplify');
const mkdirp = require('mkdirp');
const yauzl = require('yauzl');
const graphy = require('graphy');
const progress = require('progress');

// local classes
const csv_parser = require('../util/csv-parser.js');
const app_config = require('../../config.app.js');
const H_PREFIXES = app_config.prefixes;

const A_SPIN = ['◜ ◝', ' ˉ◞', ' ˍ◝', '◟ ◞', '◜ˍ ', '◟ˉ '];

// main
class Triplifier {

	constructor(p_zip_file, h_config, fk_triplify) {
		this.config = h_config;

		// prep output directory
		mkdirp('./data/output/gnis/');

		// extract basename from zip file name
		let s_basename = path.basename(p_zip_file, '.zip');

		// extract zip contents
		yauzl.open(p_zip_file, {lazyEntries:true}, (e_unzip, k_zip) => {
			if(e_unzip) throw e_unzip;

			// open zip
			k_zip.readEntry();

			// each file in zip
			k_zip.on('entry', (k_file) => {
				// indeed a csv file
				if(k_file.fileName.endsWith('.txt')) {
					// mk progress bar
					let k_bar = new progress('[:bar] :percent :spin :mib_read MiB; +:elapseds; -:etas', {
						incomplete: ' ',
						complete: '∎', // 'Ξ',
						width: 40,
						total: k_file.uncompressedSize,
					});

					// info
					local.info('parsing '+k_file.fileName);

					// // extract basename from zipped main file
					// let s_basename = path.basename(k_file.path, '.txt');

					// create serializer
					let k_serializer = graphy.ttl.serializer({
						prefixes: H_PREFIXES,
						coercions: new Map([
							[Date, (dt) => graphy.literal(dt.toISOString().replace(/T.+$/, ''), graphy.namedNode('http://www.w3.org/2001/XMLSchema#date'))],
						]),
					});

					// pipe output to file
					k_serializer.pipe(fs.createWriteStream(`./data/output/gnis/${s_basename}.ttl`));

					// pass unzip stream to csv parser
					k_zip.openReadStream(k_file, (e_read, ds_csv) => {
						if(e_read) throw e_read;

						this.parse_gnis_csv(ds_csv, k_serializer, k_bar, () => {
							k_zip.readEntry();
						});
					});
				}
			});

			// once the zip closes
			k_zip.once('close', () => {
				if(fk_triplify) fk_triplify();
			});

			//
			k_zip.on('error', (e_zip) => {
				throw 'hey: '+e_zip;
			});
		});


			// .on('close', () => {
			// 	fk_zip();
			// });
		// }, () => {
		// 	if(fk_triplify) fk_triplify();
		// });
	}


	parse_gnis_csv(ds_csv, k_serializer, k_bar, fk_csv) {
		// fetch writer
		let k_writer = k_serializer.writer;

		let c_bytes = 0;
		let n_update_bytes = 0;
		let i_spin = 0;
		let n_spin = A_SPIN.length;
		let c_updates = 0;

		// parse csv
		ds_csv.pipe(csv_parser({
			delimiter: '|',

			progress(n_bytes) {
				c_bytes += n_bytes;
				n_update_bytes += n_bytes;
				if(0 === (c_updates++ % 2)) {
					k_bar.tick(n_update_bytes, {
						mib_read: (c_bytes / 1024 / 1024).toFixed(2),
						spin: c_bytes === k_bar.total? ' ✓ ': A_SPIN[i_spin++],
					});

					i_spin = i_spin % n_spin;
					n_update_bytes = 0;
				}
			},

			// each row
			row: (h_row) => {
				let h_config = this.config;
				let h_descriptors = h_config.fields;

				// create feature
				let s_feature = h_config.subject(h_row);

				if(!s_feature) return;

				// prep pairs hash
				let h_pairs = {};

				// each property in header descriptor hash
				for(let s_property in h_descriptors) {
					// ref descriptor
					let h_descriptor = h_descriptors[s_property];

					// ref row value
					let s_value = h_row[s_property];

					// no value?!
					if(!s_value) {
						// optional
						if(h_descriptor.optional) continue;

						// otherwise; required
						throw `missing value for column "${s_property}" for feature: ${util.inspect(h_row)}`;
					}

					// function descriptor
					if('function' === typeof h_descriptor) {
						let h_add_pairs = h_descriptor.apply(null, [s_value, h_row, k_writer]);

						// returned new pairs to add
						if(h_add_pairs) {
							// merge new pairs into existing
							for(let s_predicate in h_add_pairs) {
								let a_objects = h_add_pairs[s_predicate];
								if(h_pairs[s_predicate]) h_pairs[s_predicate].push(...a_objects);
								else h_pairs[s_predicate] = a_objects;
							}
						}
					}
					// object descriptor
					else {
						// descriptor identifies simple direct predicate mapping
						if('string' === typeof h_descriptor.predicate) {
							let s_predicate = h_descriptor.predicate;

							// also direct
							if('function' === typeof h_descriptor.object) {
								// apply transformation
								let w_object = h_descriptor.object.apply(null, [s_value, h_row, k_writer]);

								// add triple
								if(w_object) {
									if(s_predicate in h_pairs) {
										h_pairs[s_predicate].push(w_object);
									}
									else {
										h_pairs[s_predicate] = [w_object];
									}
								}
							}
						}
						//
						else {
							throw `descriptor was not understood: ${util.inspect(h_descriptor)}`;
						}
					}
				}

				// write triples to output
				k_writer.add({
					[s_feature]: h_pairs,
				});
			},

			// end of csv input
			end: () => {
				k_bar.tick(n_update_bytes, {
					mib_read: (c_bytes / 1024 / 1024).toFixed(2),
					spin: c_bytes === k_bar.total? ' ✓ ': A_SPIN[i_spin++],
				});

				// end writer output
				k_serializer.close();

				// log
				fk_csv();
			},
		}));
	}
}


module.exports = function(a_inputs, h_config, fk_triplify) {
	return new Triplifier(a_inputs, h_config, fk_triplify);
};
