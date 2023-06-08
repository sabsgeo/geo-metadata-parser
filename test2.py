import json

f = open('count.json')
all_sample_from_db = json.load(f)
final_dct = {}
for k in all_sample_from_db:
    key = list(k.keys())[0]
    final_dct[key] = k.get(key)

with open('count2.json', 'w') as f:
    json.dump(final_dct, f)



