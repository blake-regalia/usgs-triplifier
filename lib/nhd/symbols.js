
const H_CODES = require('./codes.js').nhd;

const HPG_POINT = {
	proper: 'Point',
};
const HPG_MULTIPOINT = {
	proper: 'MultiPoint',
	multipart: true,
};
const HPG_LINESTRING = {
	proper: 'LineString',
	linear: true,
};
const HPG_MULTILINESTRING = {
	proper: 'MultiLineString',
	linear: true,
	multipart: true,
};
const HPG_POLYGON = {
	proper: 'Polygon',
	areal: true,
	rings: true,
};
const HPG_MULTIPOLYGON = {
	proper: 'MultiPolygon',
	areal: true,
	rings: true,
	multipart: true,
};

[
	HPG_POINT,
	HPG_MULTIPOINT,
	HPG_LINESTRING,
	HPG_MULTILINESTRING,
	HPG_POLYGON,
	HPG_MULTIPOLYGON,
].forEach(h => h.lower = h.proper.toLowerCase());


const H_TABLES_FEATURES = {
	nhdpoint: HPG_POINT,
	nhdline: HPG_MULTILINESTRING,
	nhdflowline: HPG_MULTILINESTRING,
	nhdarea: HPG_MULTIPOLYGON,
	nhdwaterbody: HPG_MULTIPOLYGON,
};

// ).reduce((h_hash, [s_key, hpg_type]) => {
// 	h_hash[s_key] = {
// 		feature: nhd_feature(s_key, hpg_type),
// 		type: hpg_type,
// 	};
// 	return h_hash;
// }, {});

const id_to_ct = s => `nhdf:${s.replace(/^{(.+)}$/, '$1').replace(/-/g, '_')}`;

const code_to_ct = (s_key, si_code) => H_CODES[s_key][+si_code].replace(/[\s-]/g, '_');


module.exports = {
	H_CODES,

	HPG_POINT,
	HPG_MULTIPOINT,
	HPG_LINESTRING,
	HPG_MULTILINESTRING,
	HPG_POLYGON,
	HPG_MULTIPOLYGON,

	H_TABLES_FEATURES,

	id_to_ct,
	code_to_ct,
};
