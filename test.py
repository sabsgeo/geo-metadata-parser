import json

f = open('pubmed_info.json')
  
data_info = json.load(f)

data_info_list = data_info.keys()
new_data_info_list = []

for typ_c in data_info_list:
    if isinstance(typ_c, int):
        new_data_info_list.append(str(typ_c))
    else:
        new_data_info_list.append(typ_c)

print(len(new_data_info_list))

f = open('series_metadataMonday_Jun_19_2023_04:22:06.json')
json_data = f.read()
each_json = json_data.split("\n")
set_me = set()
for each_row in each_json:
    pub_med = json.loads(each_row).get("Series_pubmed_id")
    set_me.update(pub_med)

final_list = list(set_me)

new_final_list = []
for typ_c in final_list:
    if isinstance(typ_c, int):
        new_final_list.append(str(typ_c))
    else:
        new_final_list.append(typ_c)

print(len(new_final_list))

k = list(set(new_final_list) - set(new_data_info_list))
print(len(k))

f = open("pub_med_diff.txt", "a")
f.write(str(k))
f.close()


