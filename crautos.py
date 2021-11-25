import logging
import pprint
import pandas as pd
import requests

from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString
from datetime import date, datetime, timedelta

formdata = {
    "brand": 35,  # 35 toyota, 16 hyundai
    "modelstr": "rav4",
    "style": 00,
    "fuel": 0,
    "trans": 0,
    "financed": 00,
    "recibe": 0,
    "province": 0,
    "doors": 0,
    "yearfrom": 2000,
    "yearto": 2022,
    "pricefrom": 100000,
    "priceto": 70000000,
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

INFO_GENERAL = [
    'Cilindrada', 'Estilo', 'Combustible', 'Transmisión', 'Estado', 'Kilometraje', 
    'Placa', 'Color exterior', 'Color interior', 'de puertas', 'Ya pagó impuestos', 
    'Precio negociable', 'Se recibe vehículo', 'Provincia', 'Fecha de ingreso'
    ]

CSV_LOCATION = 'crautos_result'


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
        if (type(tag) == NavigableString) and (tag.strip() != '') and (len(tag.strip()) < 100):
            tag_py_str = "".join(e for e in str(tag) if e.isalnum() or e == " ")
            data.append(tag_py_str)
        
def parse_fecha(fecha_str):
    fecha = fecha_str.replace('de', '').replace('l', '')
    fecha = fecha.split()
    fecha[1] = months[fecha[1]]
    fecha = ' '.join(fecha)
    fecha = datetime.strptime(fecha, '%d %m %Y').date()

    return fecha


def get_posted_cars_links():
        
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
    df_list = []
    # iterar cada car link
    for car_link in car_links:

        page = requests.get(CARDETAIL_URL.format(car_link))
        soup = BeautifulSoup(page.content, 'html.parser')

        data = {"cardetails": [], "info": [], "equipamiento": [], "detalles": []}

        read_recursive(soup.find_all('div', id='geninfo'), data["info"])
        read_recursive(soup.find_all('div', {"class":"margin-bottom-10 clearfix cardetailtitle"}), data["detalles"])

        df = pd.DataFrame({"caracteristicas":data["info"]})

        df["caracteristicas"] = df["caracteristicas"].str.strip()
        df["valor"] = df["caracteristicas"].shift(-1)
        
        df = df[df["caracteristicas"].isin(INFO_GENERAL)]
        df = df.set_index("caracteristicas")

        df = df.transpose()

        df["Modelo"] = data["detalles"][0]
        df["Año"] = data["detalles"][1]
        df["Precio Colones"] = data["detalles"][2]
        df["Precio Dolares"] = data["detalles"][3]
        
        df["link"] = CARDETAIL_URL.format(car_link)

        df_list.append(df)

        leather_car_links.append(CARDETAIL_URL.format(car_link))
    
    # concat todos los dfs
    df = pd.concat(df_list, ignore_index=True)

    df["Fecha de ingreso"] = df["Fecha de ingreso"].apply(parse_fecha)

    df["Cilindrada"] = df["Cilindrada"].fillna("0").str.extract('(\d+)').astype(int, errors="ignore")
    df["Kilometraje"] = df["Kilometraje"].fillna("0").str.extract('(\d+\.*)').astype(int, errors="ignore")


    file_datetime_id = datetime.today().strftime('%Y%m%d_%H%M%S')
    
    df.to_csv(CSV_LOCATION+file_datetime_id+".csv", index=False)
    df.to_excel(CSV_LOCATION+file_datetime_id+".xlsx", index=False)

    pp = pprint.PrettyPrinter(indent=4)
    print("car list")
    pp.pprint(leather_car_links)


def main():
    get_posted_cars_links()

if __name__ == "__main__":
    main()