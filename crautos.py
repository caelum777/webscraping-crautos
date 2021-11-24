import logging
import requests
import pprint
from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString
from datetime import date, datetime, timedelta


def isnumeric(x):
    try:
        int(x)
        return True
    except Exception:
        return False


def read_recursive(tags, data):
    tags = list(tags)
    for tag in tags:
        if type(tag) == Tag:
            read_recursive(tag.children, data)
        if type(tag) == NavigableString and tag.strip() != '' and len(tag.strip()) < 100:
            data.append(tag.encode('utf-8').strip())


formdata = {
    "brand": 35,  # 35 toyota, 16 hyundai
    "modelstr": "tercel",
    "style": 00,
    "fuel": 0,
    "trans": 0,
    "financed": 00,
    "recibe": 0,
    "province": 0,
    "doors": 0,
    "yearfrom": 1991,
    "yearto": 1999,
    "pricefrom": 1000000,
    "priceto": 2000000,
    "orderby": 0,
    "newused": 0,
    "lformat": 0,
    "l": 1
}

months = {
    'Enero': '1',
    'Febrero': '2',
    'Marzo': '3',
    'Abril': '4',
    'Mayo': '5',
    'Junio': '6',
    'Julio': '7',
    'Juio': '7',
    'Agosto': '8',
    'Setiembre': '9',
    'Octubre': '10',
    'Noviembre': '11',
    'Diciembre': '12'
}

SEARCH_URL = "https://crautos.com/autosusados/searchresults.cfm?p={}"
CARDETAIL_URL = "https://crautos.com/autosusados/{}"
BASE_URL = "https://crautos.com/autosusados/"

# Get posted cars links
car_links = []
max_page = current_page = 1
while current_page <= max_page+1:
    search_post = requests.post(SEARCH_URL.format(current_page), data=formdata)
    search_soup = BeautifulSoup(search_post.content, 'html.parser')

    # Search pagination ul list and find the max number of pages
    pagination = search_soup.find('ul', class_='pagination')
    max_page = 0
    for page in pagination.children:
        if type(page) == Tag:
            if isnumeric(page.text):
                max_page = int(page.text)

    # Search every posted car link
    car_posts = search_soup.find_all('div', class_='inventory')
    for car_post in car_posts:
        link = car_post.a.get("href")
        if link:
            car_links.append(link)
    current_page += 1

logging.info("Number of pages: [{}]".format(max_page))
leather_car_links = []
for car_link in car_links:
    page = requests.get(CARDETAIL_URL.format(car_link))
    soup = BeautifulSoup(page.content, 'html.parser')

    data = {"cardetails": [], "info": [], "equipamiento": []}

    #read_recursive(soup.find_all('div', class_='cardetailtitle'), data["cardetails"])
    read_recursive(soup.find_all('div', id='geninfo'), data["info"])
    #read_recursive(soup.find_all('div', id='equip'), data["equipamiento"])
    '''
    for option in data["equipamiento"]:
        option = option.decode('utf-8') if type(option) == bytes else option
        if "cuero" in str(option):
            leather_car_links.append(CARDETAIL_URL.format(car_link))
    '''
    for i, option in enumerate(data["info"]):
        option = option.decode('utf-8') if type(option) == bytes else option
        if "fecha" in str(option).lower():
            fecha = data["info"][i+1]
            fecha = fecha.decode('utf-8') if type(fecha) == bytes else fecha
            fecha = fecha.replace('de', '').replace('l', '')
            fecha = fecha.split()
            fecha[1] = months[fecha[1]]
            fecha = ' '.join(fecha)
            fecha = datetime.strptime(fecha, '%d %m %Y').date()
            two_days_ago = date.today() - timedelta(days=15)
            if two_days_ago <= fecha <= date.today():
                leather_car_links.append(CARDETAIL_URL.format(car_link))

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(leather_car_links)
