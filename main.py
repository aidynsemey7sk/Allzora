import pandas as pd
import json
import time
from merge import get_data_from_xml_1, get_data_from_xml_2, get_data_from_json
from fuzzywuzzy import fuzz
from time import sleep

start_time = time.time()

'''Создание трех DataFrame и сортировка'''
tr_df_1 = pd.DataFrame(get_data_from_xml_1('data_Soruce_1.xml'))  # 7062 str
tr_df_1 = tr_df_1.sort_values(by='ean_code')
tr_df_2 = pd.DataFrame(get_data_from_xml_2('data_Source_2.xml'))  # 11534
tr_df_2 = tr_df_2.sort_values(by='ean_code')
js_df = pd.DataFrame(get_data_from_json('data_Source_3.json'))  # 31368 str
js_df = js_df.sort_values(by='ean_code')

'''Объединение Dataframe по EAN коду'''
merged_inner = pd.merge(left=tr_df_1, right=tr_df_2, left_on='ean_code', right_on='ean_code')
merged_inner2 = pd.merge(left=merged_inner, right=js_df, left_on='ean_code', right_on='ean_code')

'''Получение списка ean_code которые мы уже выбрали
    для последующего удаления из начальных DF
'''
delete_list = merged_inner2['ean_code'].values.tolist()

"""Удаляем строки из начальных DF по списку для удаления"""
tr_df_1 = tr_df_1.loc[~tr_df_1['ean_code'].isin(delete_list)]  # Осталось 6318 строк
tr_df_2 = tr_df_2.loc[~tr_df_2['ean_code'].isin(delete_list)]  # Осталось 10790 строк
js_df = js_df.loc[~js_df['ean_code'].isin(delete_list)]  # Осталось 30624 строк

"""Функция объединения по производителю"""

result_list = []


def merge_manufacturer(name_manufacturer: str):
    try:
        ts = tr_df_1.loc[tr_df_1['MANUFACTURER'] == name_manufacturer]
        if name_manufacturer == "L'Oreal":
            ts = tr_df_1.loc[tr_df_1['MANUFACTURER'].str.contains("L'Or")]
        ts1 = tr_df_2.loc[tr_df_2['MANUFACTURER'] == name_manufacturer]
        ''''''
        # print(name_manufacturer)
        js_name_manufacturer = name_manufacturer.upper()
        js_name_manufacturer = js_name_manufacturer.split()
        ts2 = js_df[js_df['MANUFACTURER'].str.contains(js_name_manufacturer[0])]

        '''Создадим копию для поиска уникальных строк'''
        all_manufacturer_name = ts2.copy()
        '''Вырежем все дубликаты имён производителей'''
        unique_manufacturer_name = all_manufacturer_name.drop_duplicates(subset=['MANUFACTURER'])
        # print(unique_manufacturer_name)
        '''Получим уникальное количество производителей'''
        count_unique_name = unique_manufacturer_name.shape[0]
        manufacture_name = unique_manufacturer_name.iloc[0]['MANUFACTURER']

        '''Создадим и заполним его всеми вариантами имени производителся'''

        appended_data = []
        if count_unique_name > 1:
            for i in range(count_unique_name):
                new_df = js_df.loc[js_df['MANUFACTURER'] == unique_manufacturer_name.iloc[i]['MANUFACTURER']]
                appended_data.append(new_df)
        '''Объеденим в итоговый DF'''
        if appended_data:
            appended_data = pd.concat(appended_data)
            ts2 = appended_data

    except Exception as e:
        print(f'[ERROR] {str(e)} Такого бренда нет в одном из датафреймов')
    row1_ean_list = []
    row_ean_list = []
    not_duplicate_ean_list = []
    two_ean_code = []

    for i, row in ts.iterrows():
        brand_list = row['brandline'].split()
        if row['tester']:
            brand_list.append('tester')
        size = row['SIZE']

        """Обработка DF 2"""
        fuzzi_list = []
        fuzzi_list_2 = []
        for x, row1 in ts1.iterrows():
            size1 = row1['SIZE']
            fuzz_data = row['name']
            fuzz_data_1 = row1['name'] + ' ' + row1['SIZE'] + ' ' + row1['measure'] + ' ' + row1['gender']
            ean_code = row['ean_code']

            ''' Получим список с двумя словарями, равными по ean_code,
                В список row_ean_list запишем еan_code которые уже попали в итоговый список, что бы знать какие 
                строки уже не надо обрабатывать, чем кратно увеличим скорость обработки.
            '''
            if str(row['ean_code']) == str(row1['ean_code']):
                row_ean_list.append(row['ean_code'])
                m = [
                    {
                        'ean_code': row['ean_code'], 'name': row['name'],
                        "source_name": row['source_name'], 'id': row['id'],
                        'SIZE': row['SIZE']
                    },
                    {
                        'ean_code': row1['ean_code'], 'name': row1['name'],
                        "source_name": row1['source_name'], 'id': row1['id']
                    }
                ]
                '''Сформируем данныe для сравнения в третьем DF'''
                two_ean_code.append(m)

            '''Выберем все вариации с рейтингом больше 80 и проверим на вхождение в список row_ean_list
            запишем в список fuzzi_list'''
            is_good_rating: bool = fuzz.token_sort_ratio(fuzz_data, fuzz_data_1) > 80
            is_ean_code_not_in_list: bool = row['ean_code'] not in row1_ean_list

            if is_good_rating:
                if size == size1 and is_ean_code_not_in_list:
                    rating = fuzz.token_sort_ratio(fuzz_data, fuzz_data_1)
                    fuzzi_list.append([
                        {
                            'ean_code': ean_code, 'fuzz_data': fuzz_data, 'name': row['name'],
                            'id': row['id'], 'source_name': row['source_name']
                        },

                        {
                            'ean_code': row1['ean_code'], 'fuzz_data_1': fuzz_data_1, 'id': row1['id'],
                            'source_name': row1['source_name'], 'name': row1['name'], 'rating': rating
                        }
                    ])

                    row1_ean_list.append(row1['ean_code'])

        '''Из списка fuzzi_list выберем значения с наибольшим рейтингом'''
        if fuzzi_list:
            if len(fuzzi_list) > 1:
                sorted_salaries = sorted(fuzzi_list, key=lambda d: d[1]['rating'])
                res = sorted_salaries[-1]
            else:
                res = fuzzi_list[0]

            """Обработка DF 3"""
            for x, row2 in ts2.iterrows():
                fuzz_data_2 = row2['MANUFACTURER'] + ' ' + row2['name']

                '''Сначала обработаем данные из two_ean_code,
                 так как они уже совпали у df1 и df2'''
                two_ean_fuzzy_list = []
                for z in two_ean_code:
                    new_fuzz_data = z[0]['name']
                    is_good_rating_for_fuzz_data_2: bool = fuzz.token_sort_ratio(new_fuzz_data, fuzz_data_2) > 71
                    is_ean_code_not_in_list: bool = row['ean_code'] not in row_ean_list

                    if is_good_rating_for_fuzz_data_2:
                        if str(row2['SIZE']) == str(z[0]['SIZE']) and is_ean_code_not_in_list:
                            new_rating = fuzz.token_sort_ratio(new_fuzz_data, fuzz_data_2)
                            two_ean_fuzzy_list.append(
                                [{
                                    'source_name': z[0]['source_name'],
                                    'name': z[0]['name'],
                                    'ean_code': z[0]['ean_code'],
                                    'id': z[0]['id'],
                                },
                                    {
                                        'source_name': z[1]['source_name'],
                                        'name': z[1]['name'],
                                        'ean_code': z[1]['ean_code'],
                                        'id': z[1]['id'],
                                    },
                                    {
                                        'source_name': row2['source_name'],
                                        'name': row2['name'],
                                        'ean_code': row2['ean_code'],
                                        'id': row2['id'],
                                        'rating': new_rating

                                    }
                                ]
                            )
                '''Проверим и отправим результат'''
                if two_ean_fuzzy_list:
                    if len(two_ean_fuzzy_list) == 1:
                        not_duplicate_ean_list.append(two_ean_fuzzy_list['ean_code'])
                        break
                    if len(two_ean_fuzzy_list) > 1:
                        sorted_salaries = sorted(two_ean_fuzzy_list, key=lambda d: d[2]['rating'])
                        if sorted_salaries[-1][2]['ean_code'] not in not_duplicate_ean_list:
                            not_duplicate_ean_list.append(sorted_salaries[-1][2]['ean_code'])
                            res.append(sorted_salaries[-1])

                '''Выберем все вариации с рейтингом больше 68 и проверим на вхождение в список row_ean_list
                запишем в список fuzzi_list'''
                if fuzz.token_sort_ratio(res[0]['fuzz_data'], fuzz_data_2) > 58:
                    if size in fuzz_data_2.split():
                        rating = fuzz.token_sort_ratio(res[0]['fuzz_data'], fuzz_data_2)
                        if 'tester' in res[0]['fuzz_data'].split():
                            rating = rating - 1
                        fuzzi_list_2.append([
                            {'ean_code': ean_code, 'fuzz_data': fuzz_data, 'name': row2['name'], 'id': row1['id'],
                             'source_name': row1['source_name']},
                            {'ean_code': row2['ean_code'], 'fuzz_data_1': fuzz_data_2, 'name': row2['name'],
                             'id': row2['id'],
                             'name': row2['name'],
                             "source_name": row2['source_name'], 'rating': rating}
                        ])

            '''Проверим и отправим результат'''
            if fuzzi_list_2:
                if len(fuzzi_list_2) > 1:
                    sorted_salaries = sorted(fuzzi_list_2, key=lambda d: d[1]['rating'])

                    res1 = sorted_salaries[-1]
                    if res1[0]['ean_code']:
                        res.append(res1[-1])
                else:
                    if res1[0]['ean_code']:
                        res1 = fuzzi_list_2[0]
                        res.append(res1[-1])

            if len(res) > 2:
                result_list.append(res)
                return res


'''Запустим цикл для получения объединенных DF по имени производителя'''
cnt = 0
not_duplicate_list = []
compare_products = []
all_manufacturer_name = tr_df_1
unique_manufacturer_name = all_manufacturer_name.drop_duplicates(subset=['MANUFACTURER'])
for i, row4 in unique_manufacturer_name.iterrows():
    brand = row4['MANUFACTURER']
    try:
        if brand not in not_duplicate_list:
            result = merge_manufacturer(brand)
            result_list.append(result)
            cnt += 1
            not_duplicate_list.append(brand)
    except Exception as ex:
        print(f'{ex} - Этот бренд мы уже обходили')

# merge_manufacturer("L'Oreal")
print(len(result_list))  # 77

print(merged_inner2.shape)
'''Распакуем объединенный список и прибавим к итоговому списку'''
for i, row in merged_inner2.iterrows():
    res = [
        {
            'source_name': row['source_name_x'],
            'name': row['name_x'],
            'ean_code': row['ean_code'],
            'id': row['id_x'],
        },
        {
            'source_name': row['source_name_y'],
            'name': row['name_y'],
            'ean_code': row['ean_code'],
            'id': row['id_y'],
        },
        {
            'source_name': row['source_name'],
            'name': row['name'],
            'ean_code': row['ean_code'],
            'id': row['id'],
        },
    ]
    result_list.append(res)
clear_data_list = []
print(len(result_list))
cont = 0
# print(result_list)
for i in result_list:
    cont += 1
    if i[0]:
        clear_result = [
            {
                'source_name': i[0]['source_name'],
                'name': i[0]['name'],
                'ean_code': i[0]['ean_code'],
                'id': i[0]['id'],
            },
            {
                'source_name': i[1]['source_name'],
                'name': i[1]['name'],
                'ean_code': i[1]['ean_code'],
                'id': i[1]['id'],
            },
            {
                'source_name': i[2]['source_name'],
                'name': i[2]['name'],
                'ean_code': i[2]['ean_code'],
                'id': i[2]['id'],
            },
        ]
    clear_data_list.append(clear_result)
print(len(clear_data_list))
with open("db.json", "w") as file:
    json.dump({'compare_products': clear_data_list}, file)
print("--- %s seconds -func-" % (time.time() - start_time))
