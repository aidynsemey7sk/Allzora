
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP tester 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE EDP spray 75 ml'))
print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP tester 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE eau de parfum spray spray 75 ml'))

print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP tester 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE eau de parfum spray 75 ml'))
# print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE eau de parfum spray spray 75 ml'))

print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE EDP spray 75 ml'))
print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE eau de parfum spray spray 75 ml'))

print(fuzz.token_sort_ratio('Hugo Boss Ma Vie Pour Femme Edp Spray 75 ml W', 'HUGO BOSS-BOSS BOSS MA VIE eau de parfum spray 75 ml'))
# print(fuzz.token_sort_ratio('Hugo Boss Ma Vie Pour Femme Edp Spray 75 ml W', 'BOSS MA VIE eau de parfum spray spray 75 ml'))

# print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP 75 ml W', 'Hugo Boss Ma Vie Pour Femme Edp Spray 75,00 ml W'))
#
# print(fuzz.token_sort_ratio('Hugo Boss Boss Ma Vie Pour Femme EDP 75 ml W', 'Hugo Boss Ma Vie Pour Femme Edp Spray 30 ml W'))

# HUGO BOSS-BOSS

a = [[{'source_name': 'data_Source_1', 'name': 'Hugo Boss Boss The Scent For Him DEO ve spreji 150 ml M', 'ean_code': '737052992785', 'id': '81332'}, {'source_name': 'data_Source_2', 'name': 'Hugo Boss The Scent Deo Spray', 'ean_code': '737052992785', 'id': 'R-DV-253-B6'}, {'source_name': 'data_Source_3', 'name': 'THE SCENT deodorant spray 150 ml', 'ean_code': '0737052992785', 'id': '72025'}, {'rating': 80}],
     [{'source_name': 'data_Source_1', 'name': 'Hugo Boss Boss The Scent For Him SG 150 ml M', 'ean_code': '737052992860', 'id': '81734'}, {'source_name': 'data_Source_2', 'name': 'Hugo Boss The Scent Shower Gel', 'ean_code': '737052992860', 'id': 'R-DV-600-B6'}, {'source_name': 'data_Source_3', 'name': 'THE SCENT deodorant spray 150 ml', 'ean_code': '0737052992785', 'id': '72025'}, {'rating': 78}],
[{'source_name': 'data_Source_1', 'name': 'Hugo Boss Boss The Scent For Him SG 150 ml M', 'ean_code': '737052992860', 'id': '81734'}, {'source_name': 'data_Source_2', 'name': 'Hugo Boss The Scent Shower Gel', 'ean_code': '737052992860', 'id': 'R-DV-600-B6'}, {'source_name': 'data_Source_3', 'name': 'THE SCENT deodorant spray 150 ml', 'ean_code': '0737052992785', 'id': '72025'}, {'rating': 79}]
     ]
# print(a[0][-1]['rating'])

# sorted(a['rating'], key=lambda d: d)
# print(a)
x = a[0]
for i in a:
    if i[-1]['rating'] > x[-1]['rating']:
        x = i
print(x)


