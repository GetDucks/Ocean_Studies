input_file = '../sources/space_missions.log'
output_file = '../sources/space_missions_cleaned.csv'

with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
    for line in infile:
        if line.strip().startswith('#') or 'CHECKPOINT' in line or 'SYSTEM:' in line or not line.strip():
            continue
        fields = [field.strip() for field in line.split('|')]
        if len(fields) == 8:
            outfile.write(','.join(fields) + '\n')
