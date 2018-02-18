
const pg_cursor = require('pg-cursor');

const N_CORES = require('os').cpus().length;

// make a new range
const cursor_range = async (h_config) => {
	let {
		pool: y_pool,
		query: s_query,
		data: f_data,
		lo: i_lo,
		hi: i_hi,
		update: n_updates=16,
	} = h_config;

	// checkout a client
	let y_client = await y_pool.connect();

	// compute range size
	let n_range = i_hi - i_lo;

	// determine chunk size
	let n_chunk_size = Math.ceil(n_range / n_updates);

	// create cursor on range
	let y_cursor = y_client.query(new pg_cursor(`${s_query} limit ${n_range} offset ${i_lo}`));

	// next cursor read
	return new Promise((fk_consume) => {
		(function f_read() {
			y_cursor.read(n_chunk_size, async (e_read, a_rows) => {
				if(e_read) throw e_read;

				// each row serially
				await f_data(a_rows);

				// more to consume
				if(n_chunk_size === a_rows.length) {
					setImmediate(f_read);
				}
				// all done
				else {
					// release client back to pool
					y_client.release();

					// complete promise
					fk_consume();
				}
			});
		})();
	});
};

// 
const splitter = async (h_config) => {
	let {
		pool: y_pool,
		rows: n_rows,
		query: s_query,
		data: f_data,
		cores: n_cores=N_CORES,
		updates: n_updates=256,
	} = h_config;

	// size of each range
	let n_range_size = Math.ceil(n_rows / n_cores);

	// all ranges
	let a_promises = [];

	// initiate lo/hi
	let i_lo = 0;
	let i_hi = n_range_size;

	// mk ranges
	while(i_lo < n_rows) {
		// add ragne
		a_promises.push(cursor_range({
			pool: y_pool,
			query: s_query,
			data: f_data,
			lo: i_lo,
			hi: i_hi,
			updates: Math.ceil(n_updates / n_cores),
		}));

		// advance lo
		i_lo = i_hi;

		// increment hi
		i_hi = Math.min(i_hi+n_range_size, n_rows);
	}

	// await
	await Promise.all(a_promises);
};

module.exports = splitter;
