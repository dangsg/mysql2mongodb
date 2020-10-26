def extract_dict(selected_keys):
	def extract_dict(input_dict):
		output_dict = {}
		for key in selected_keys:
			output_dict[str(key)] = input_dict[str(key)]
		return output_dict
	return extract_dict