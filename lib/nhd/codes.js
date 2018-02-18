
function pp(h, s_post, s_pre='nhd:') {
	for(let s_key in h) {
		h[s_key] = s_pre+h[s_key]+s_post;
	}
	return h;
}

module.exports = {
	nhd: {
		point_event: pp({
			57001: 'Continuously Active Streamgage',
			57002: 'Partially Active Streamgage',
			57003: 'Inactive Streamgage',
			57004: 'Water Quality Station',
			57100: 'Dam',
			57201: 'Flow Alteration Addition',
			57202: 'Flow Alteration Removal',
			57203: 'Flow Alteraion Unknown',
			57300: 'Hydrologic Unit Outlet',
		}, '_Point_Event'),

		organization: pp({
			0: 'Unknown',
			1: 'International',
			2: 'Federal',
			3: 'Tribal',
			4: 'State',
			5: 'Regional',
			6: 'County',
			7: 'Municipal',
			8: 'Private',
		}, '_Organization'),

		resolution: pp({
			1: 'Local',
			2: 'High',
			3: 'Medium',
		}, '_Resolution'),

		hydro_flow_direction: pp({
			0: 'Uninitialized',
			1: 'With Digitied',
		}, '_Hydro_Flow_Direction'),

		flow_direction: pp({
			709: 'In',
			712: 'Network Start',
			713: 'Network End',
			714: 'Non-Flowing',
		}, '_Flow_Direction'),
	},
};

