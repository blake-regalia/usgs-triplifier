const stream = require('stream');

class CsvParser extends stream.Writable {
	constructor(h_opt) {
		// construct super's writable stream
		super();

		// header options
		let r_capture_header = /^[^\w]+(.*)"?$/;
		let s_replace_header = '$1';
		let f_map_header = s => s.toLowerCase();
		if(h_opt.headers) {
			let h_headers = h_opt.headers;
			if(h_headers.capture) r_capture_header = h_headers.capture;
			if(h_headers.replace) s_replace_header = h_headers.relace;
			if(h_headers.map) f_map_header = h_headers.map;
		}

		// data options
		let r_capture_data = /^"(.*)"$/;
		let s_replace_data = '$1';
		if(h_opt.data) {
			let h_data = h_opt.data;
			if(h_data.capture) r_capture_data = h_data.capture;
			if(h_data.replace) s_replace_data = h_data.relace;
		}

		// this
		Object.assign(this, {
			headers: null,
			pre: '',
			delimiter: h_opt.delimiter || ',',
			clean_header: (s_header) => f_map_header(s_header.replace(r_capture_header, s_replace_header)),
			clean_data: (s_data) => s_data.replace(r_capture_data, s_replace_data),
			row: 0,
			handle_row: h_opt.row,
			progress: h_opt.progress,
		});

		// set encoding of input stream
		this.on('pipe', (ds_src) => {
			ds_src.setEncoding('utf8');
			ds_src.on('end', () => {
				h_opt.end();
			});
		});
	}

	_write(s_chunk, s_encoding, fk_chunk) {
		let n_bytes = s_chunk.length;
		if('utf8' !== s_encoding) s_chunk = s_chunk.toString('utf8');
		let a_headers = this.headers;
		let s_delimiter = this.delimiter;
		let f_clean_data = this.clean_data;
		let f_handle_row = this.handle_row;
		let i_row = this.row;

		// update unprocessed string
		this.pre += s_chunk;

		// split by newline
		let a_lines = this.pre.split(/\r?\n/);

		// at least one complete line present
		if(a_lines.length > 1) {
			// headers not yet set
			if(!a_headers) {
				a_headers = this.headers = a_lines.shift().split(s_delimiter).map(this.clean_header);
			}

			// each line except the last
			for(let i_line=0; i_line<a_lines.length-1; i_line++) {
				// split by delimiter
				let a_cells = a_lines[i_line].split(s_delimiter).map(f_clean_data);

				// mk cell hash
				let h_row = {};
				a_cells.forEach((s_cell, i_field) => h_row[a_headers[i_field]] = s_cell);

				// call event handler
				f_handle_row(h_row, i_row, a_cells);

				// increment row counter
				i_row += 1;
			}

			// update row index
			this.row = i_row;

			// update unprocessed bit
			this.pre = a_lines[a_lines.length-1];
		}

		// progress event
		if(this.progress) {
			this.progress(n_bytes);
		}

		// done with chunk
		fk_chunk();
	}
}

module.exports = function(...a_args) {
	return new CsvParser(...a_args);
};
