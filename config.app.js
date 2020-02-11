
const S_DATA_HOST = process.env.USGS_DATA_HOST || 'usgs.link';
const S_DATA_PATH = process.env.USGS_DATA_PATH || '';

const P_DATA_URI = `https://${S_DATA_HOST}${S_DATA_PATH}`;
const P_RDF_URI = `${P_DATA_URI}/rdf`;
const P_GEOM_URI = `${P_DATA_URI}/geometry`;

module.exports = {
	data_uri: P_DATA_URI,
	rdf_uri: P_RDF_URI,
	geom_uri: P_GEOM_URI,
	prefixes: {
		rdf: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
		rdfs: 'http://www.w3.org/2000/01/rdf-schema#',
		owl: 'http://www.w3.org/2002/07/owl#',
		xsd: 'http://www.w3.org/2001/XMLSchema#',
		qudt: 'http://qudt.org/schema/qudt/',
		unit: 'http://qudt.org/vocab/unit/',
		geosparql: 'http://www.opengis.net/ont/geosparql#',
		ago: 'http://awesemantic-geo.link/ontology/',
		geonames: 'http://sws.geonames.org/',

		usgs: `${P_RDF_URI}/usgs/ontology/`,
		gnis: `${P_RDF_URI}/gnis/ontology/`,
		gnisf: `${P_RDF_URI}/gnis/feature/`,
		'gnisf-alias': `${P_RDF_URI}/gnis/feature-alias/`,
		nhd: `${P_RDF_URI}/nhd/ontology/`,
		nhdf: `${P_RDF_URI}/nhd/feature/`,
		cegis: `${P_RDF_URI}/cegis/ontology/`,
		cegisf: `${P_RDF_URI}/cegis/feature/`,
		'usgeo-point': `${P_GEOM_URI}/point/`,
		'usgeo-multipoint': `${P_GEOM_URI}/multipoint/`,
		'usgeo-linestring': `${P_GEOM_URI}/linestring/`,
		'usgeo-multilinestring': `${P_GEOM_URI}/multilinestring/`,
		'usgeo-polygon': `${P_GEOM_URI}/polygon/`,
		'usgeo-multipolygon': `${P_GEOM_URI}/multipolygon/`,
	},
};
