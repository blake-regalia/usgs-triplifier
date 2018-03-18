
const pg_cursor = require('pg-cursor');

const N_CORES = require('os').cpus().length;

// make a new range
const cursor_range = async (h_config) => {
	let {
		pool: y_pool,
		query: f_query,
		data: f_data,
		context: f_context,
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

	// make query
	let s_query = f_query(n_range, i_lo);

	// create cursor on range
	let y_cursor = y_client.query(new pg_cursor(s_query));

	// no data, create using context
	let fk_context;
	if(!f_data) [f_data, fk_context] = f_context(n_range, i_lo);

	// next cursor read
	return new Promise((fk_consume) => {
		(function f_read() {
			y_cursor.read(n_chunk_size, async (e_read, a_rows) => {
				if(e_read) throw e_read;

				// all done
				if(!a_rows.length) {
					// release client back to pool
					y_client.release();

					if(fk_context) fk_context();

					// complete promise
					fk_consume();
				}
				else {
					// each row serially
					let b_stop = await f_data(a_rows);

					// stop
					if(b_stop) {
						if(fk_context) fk_context();
						return fk_consume();
					}

					// more to consume
					setImmediate(f_read);
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
		query: f_query,
		data: f_data,
		context: f_context,
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
			query: f_query,
			data: f_data,
			context: f_context,
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
