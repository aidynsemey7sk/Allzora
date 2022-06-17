import xml.etree.ElementTree as ET
import json
import time

start_time = time.time()


def get_data_from_xml_1(xml_doc):
    count = 0
    tree = ET.parse(xml_doc)
    et_root = tree.getroot()
    all_products = et_root.findall('SHOPITEM')
    for product in all_products:
        products = product
        count += 1
        try:
            if products.find('EAN').text:
                ean = products.find('EAN').text
            else:
                ean = ''
        except Exception as ex:
            ean = ''

        id = products.find('id').text

        if products.find('MANUFACTURER').text:
            manufacturer = products.find('MANUFACTURER').text
        else:
            manufacturer = ''
        if products.find('SIZE').text and products.find('SIZE').text != 'ml':
            size = products.find('SIZE').text
        else:
            size = ''
        if products.find('NAME').text:
            name = products.find('NAME').text
        else:
            name = ''
        if products.find('RANGE').text:
            brand_line = products.find('RANGE').text
        else:
            brand_line = ''

        find_sort = name.split()

        if 'tester' in find_sort:
            tester = True
        else:
            tester = False

        size = size.split(".")[0]

        dct = {'id': id, 'ean_code': ean, 'MANUFACTURER': manufacturer, 'brandline': brand_line,
               'name': name, 'SIZE': size, 'source_name': 'data_Source_1', 'tester': tester
               }
        yield dct


def get_data_from_xml_2(xml_doc):
    count = 0
    tree = ET.parse(xml_doc)
    et_root = tree.getroot()
    all_products = et_root.findall('Product')
    for product in all_products:
        try:
            products = product
            ean = products.find('EAN').text
            manufacturer = products.find('Brand').text
            id = products.find('id').text
            weight = products.find('Weight').text
            brand_line = products.find('BrandLine').text
            description = products.find('Description').text
            category = product.find('StockType').text
            sort = product.find('Sort').text
            measure = product.find('Weight_UnitOfMeasurement').text
            gender = product.find('Gender').text
            if gender == 'H':
                gender = 'M'
            elif gender == 'D':
                gender = 'W'
            else:
                gender = 'U'

            if products.find('ProductTranslation'):
                name = products.find('ProductTranslation').find('name').text
            else:
                name = ''
            weight = weight.split(",")[0]

            dct = {'id': id, 'ean_code': ean,
                   'name': name, 'MANUFACTURER': manufacturer, 'brandline': brand_line, 'measure': measure,
                   'description': description, 'category': category, 'sort': sort, 'gender': gender,
                   'SIZE': weight, 'source_name': 'data_Source_2'}
            yield dct
            count += 1
        except Exception as er:
            print(er)


def get_data_from_json(json_doc):
    with open(json_doc) as f:
        all_products = json.load(f, strict=False)
    for product in all_products:
        ean_code = str(product['EANs'][0])
        id = str(product['Id'])
        name = str(product['name'])
        size = product['Contenido']
        size = size.split()
        measure = ' '
        if size:
            try:
                measure = size[1]
                size = int(size[0])
            except Exception as ex:
                size = size[0]
        if size == 'ml':
            size = 0
        manufacturer = product['BrandName']
        brand_line = product['LineaName']
        brand_id = product['BrandId']

        dct = {'id': id, 'ean_code': ean_code, 'MANUFACTURER': manufacturer, 'brandline': brand_line,
               'name': name, 'SIZE': size, 'brand_id': brand_id, 'measure': measure, 'source_name': 'data_Source_3',
               }
        yield dct
