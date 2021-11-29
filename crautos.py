import json
import logging
import pandas as pd
import requests

from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString
from datetime import date, datetime, timedelta


formdata = {
    "brand": 97,  # 35 toyota, 16 hyundai, 15 honda, 97 BYD,
    "modelstr": "",
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
    'Abri' : '4',
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

COLUMNS_ORDER = [
    'Modelo', 'Estilo', 'Combustible', 'Transmisión', 
    'Año','Cilindrada', 'Kilometraje',  'Precio Colones', 'Precio Dolares', 'Precio negociable',
    'Placa', 'Color exterior', 'Color interior', 'Ya pagó impuestos', 'Provincia', 'Estado',
    'Fecha de ingreso', 'link', 'Se recibe vehículo', 'de puertas'
    ]

CSV_LOCATION = 'data/crautos_result'


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
        
def parse_date(date_str):
    date_ = date_str.replace('de', '').replace('l', '')
    date_ = date_.split()
    date_[1] = months[date_[1]]
    date_ = ' '.join(date_)
    date_ = datetime.strptime(date_, '%d %m %Y').date()

    return date_


def extract_str_to_int(series: pd.Series) -> pd.Series:
    series = pd.to_numeric((series.str.extract('(\d+)', expand=False)), errors="coerce", downcast="integer")
    series = series.fillna(0).astype(int)

    return series


def get_posted_cars_links(formdata):
        
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

    return car_links


def pull_info_from_car_link(car_link) -> pd.DataFrame:

    page = requests.get(CARDETAIL_URL.format(car_link))
    soup = BeautifulSoup(page.content, 'html.parser')

    data = {"cardetails": [], "info": [], "equipamiento": [], "detalles": []}

    read_recursive(soup.find_all('div', id='geninfo'), data["info"])
    read_recursive(soup.find_all('div', {"class":"margin-bottom-10 clearfix cardetailtitle"}), data["cardetails"])

    df = pd.DataFrame({"caracteristicas":data["info"]})

    df["caracteristicas"] = df["caracteristicas"].str.strip()
    df["valor"] = df["caracteristicas"].shift(-1)
    
    df = df[df["caracteristicas"].isin(INFO_GENERAL)]
    df = df.set_index("caracteristicas")

    df = df.transpose()

    df["Modelo"] = data["cardetails"][0]
    df["Año"] = data["cardetails"][1]
    df["Precio Colones"] = data["cardetails"][2]
    df["Precio Dolares"] = data["cardetails"][3]
    
    df["link"] = CARDETAIL_URL.format(car_link)

    local_columns = [
        'Cilindrada', 'Estilo', 'Combustible', 'Transmisión', 'Estado', 'Kilometraje', 
        'Placa', 'Color exterior', 'Color interior', 'de puertas', 'Ya pagó impuestos', 
        'Precio negociable', 'Se recibe vehículo', 'Provincia', 'Fecha de ingreso', 'Modelo', 
        'Año', 'Precio Colones', 'Precio Dolares', 'link']

    df = df[local_columns]


    return df




def iter_car_links(car_links) -> pd.DataFrame:

    df_list = []

    # iter every car link
    for car_link in car_links:
        try:
            df_car = pull_info_from_car_link(car_link)
            df_car = df_car.reset_index(drop=True)
            df_list.append(df_car)
        except Exception as exc:
            print(exc)
    
    # concat every dfs

    try:
        df = pd.concat(df_list)
    except Exception as exc:
        print(exc)
        for df1 in df_list:
            print(df1.shape, df1["Modelo"])
            print(df1["link"])
        return pd.DataFrame()


    df["Fecha de ingreso"] = df["Fecha de ingreso"].apply(parse_date)

    df["Cilindrada"] = extract_str_to_int(df["Cilindrada"])
    df["Kilometraje"] = extract_str_to_int(df["Kilometraje"])
    df["Precio Colones"] = extract_str_to_int(df["Precio Colones"])
    df["Precio Dolares"] = extract_str_to_int(df["Precio Dolares"])
    df["Año"] = extract_str_to_int(df["Año"])


    return df


def save_to_file(df:pd.DataFrame):
    
    df = df[COLUMNS_ORDER]

    file_datetime_id = datetime.today().strftime('%Y%m%d_%H%M%S')
    df.to_excel(CSV_LOCATION+file_datetime_id+".xlsx", index=False)


def read_car_brands():
    car_brands: dict
    try: 
        with open("car_brands.json") as json_file:
            car_brands = json.load(json_file)
    except Exception as exc:
        print(exc)
    finally:
        return car_brands


def main():
    # 35 toyota, 16 hyundai, 15 honda, 26 nissan, 17 isuzu, 34 suzuki, 19 kia
    custom_brand_query = [

        {"brand": 34, "modelstr": "vitara"},
        {"brand": 35, "modelstr": "hilux"},


        {"brand": 35, "modelstr": "rav4"},
        {"brand": 35, "modelstr": "yaris"},
        
        {"brand": 35, "modelstr": "corolla"},
        
        {"brand": 16, "modelstr": "tucson"},
        {"brand": 16, "modelstr": "santa fe"},
        {"brand": 16, "modelstr": "santa fe"},
        {"brand": 16, "modelstr": "creta fe"},

        {"brand": 15, "modelstr": "crv"},

        {"brand": 26, "modelstr": "kiks"},
        {"brand": 26, "modelstr": "qashqai"},
        {"brand": 26, "modelstr": "xtrail"},


        {"brand": 17, "modelstr": "dmax"},

        

        {"brand": 19, "modelstr": "sportage"},
        {"brand": 19, "modelstr": "sorento"},
        
        
    ]

    df_cars = []
    car_brands = read_car_brands()
    for e, custom_query in enumerate(custom_brand_query):


        brand = car_brands.get(str(custom_query["brand"]), "Sin identificar")
        car_model = custom_query.get("modelstr", "Sin identificar")

        print(f"Iteration {e} of {len(custom_brand_query)}. Brand {brand} model: {car_model}")

        formdata_local = formdata
        formdata_local.update(custom_query)

        car_links = get_posted_cars_links(formdata_local)

        df = iter_car_links(car_links)

        df["Marca"] = brand

        df_cars.append(df)

        # if e == 2:
        #     break

    if df_cars :
    
        df_cars = pd.concat(df_cars)

        df_cars = df_cars.reset_index(drop=True)

        save_to_file(df_cars)
    else:

        print("WTF no cars")


if __name__ == "__main__":
    main()
