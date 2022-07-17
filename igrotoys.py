import collections
from xml.etree.ElementTree import fromstring

from xmljson import badgerfish as bf
import requests

from utils import Parser

symbols = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&\'()*+,-./:;<=>?абвгдеёжзийклмнопрстуфхцчшщъыьэюяЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ"

URL = "https://igrotoys.ru/modules/shop/shop.yml3.php"
parser_name = "igrotoys"
links = ''

parser = Parser(parser_name)


def main():
    print("Начало")
    res_json = xml_to_json(URL)
    parser.append_start()
    if parser.isParseCat:
        print("Парсим категорий")
        send_cats(res_json)
    print("Парсим товары")
    send_products(res_json)
    parser.append_end(links)
    print("Конец")


def xml_to_json(url):
    response = requests.get(url)
    return bf.data(fromstring(response.content))


def get_categories(res_json) -> dict:
    """
     :key    -> :value
     $       -> text
     @id     -> int
    """
    for cat in res_json['yml_catalog']["shop"]["categories"]['category']:
        cat['name'] = cat["$"]
        if "@parentId" in cat:
            split = cat["@parentId"].split()
            cat["href"] = split[1].split("=")[-1]
            cat["parent_href"] = split[2].split("=")[-1]
        elif "@id" in cat:
            split = cat["@id"].split()
            cat["href"] = split[1].split("=")[-1]
            cat["parent_href"] = ""
        else:
            print("no found href for category", cat)
            cat["href"] = ""
            cat["parent_href"] = ""
        yield cat


def get_products(res_json) -> dict:
    """
         :key                  -> :value    \n
         @id                   -> text      \n
         @available            -> bool      \n
         @bid                  -> int       \n
         url                   -> url       \n
         price                 -> int       \n
         currencyId            -> int       \n
         categoryId            -> int       \n
         picture               -> [url]     \n
         delivery              -> bool      \n
         name                  -> text      \n
         manufacturer_warranty -> bool      \n
         description           -> text      \n
    """

    for product in res_json['yml_catalog']["shop"]["offers"]["offer"]:
        for k, v in product.items():
            product[k] = v["$"] if isinstance(v, collections.OrderedDict) else v
        if "description" not in product:
            product["description"] = ""
        if "picture" not in product:
            product["picture"] = []
        elif not isinstance(product["picture"], list):
            try:
                product["picture"] = [product["picture"]]
            except:
                print("Ошибка с picture")
                product['picture'] = []

        product['pagetitle'] = ''.join(filter(lambda value: value in symbols, list(product["name"])))
        product['content'] = ''.join(filter(lambda value: value in symbols, list(product["description"])))
        product["parent"] = "https://igrotoys.ru/parserdlya-devochek/"
        yield product


def send_cats(res_json):
    for cat in get_categories(res_json):
        parser.append_category({
            "name": cat["$"],
            "url": cat["href"],
            "parent": cat["parent_href"]
        })


def send_products(res_json):
    global links
    for product in get_products(res_json):
        try:
            parser.append_product({
                "price": int(product["price"]),
                "link": product["url"],
                "pagetitle": product['pagetitle'],
                "content": product['content'],
                "article": product["vendorCode"],
                "imgs": ",".join(product["picture"]) + ",",
                "count": product["quantity_in_stock"],
                "parent": product["parent"],
            })
        except Exception as e:
            print("Ошибка, продукт не спарсился", product)
            print(e)
        links += product["url"] + '||'


if __name__ == '__main__':
    main()
