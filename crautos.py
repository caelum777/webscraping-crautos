import json
import logging
import numpy as np
import pandas as pd
import requests

from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString
from datetime import date, datetime, timedelta

from PIL import Image
from io import BytesIO

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
PHOTO_URL = "https://crautos.com/{}"

INFO_GENERAL = [
    'Cilindrada', 'Estilo', 'Combustible', 'Transmisión', 'Estado', 'Kilometraje', 
    'Placa', 'Color exterior', 'Color interior', 'de puertas', 'Ya pagó impuestos', 
    'Precio negociable', 'Se recibe vehículo', 'Provincia', 'Fecha de ingreso'
    ]

COLUMNS_ORDER = [
    'Marca', 'Modelstr', 'link', 'Estilo', 'Combustible', 'Transmisión', 
    'Año','Cilindrada', 'Kilometraje', 'KM por año', 'Precio Colones', 'Precio Dolares', "Dias desde publicado", 
    'Precio negociable', 'Placa', 'Color exterior', 'Color interior', 'Ya pagó impuestos', 'Provincia', 'Estado',
    'Fecha de ingreso', 'Se recibe vehículo', 'de puertas', 'Modelo'
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

def read_photos(tags, data):
    # TODO
    photos_list = []
    for tag in tags:
        photo_src = tag.get("src", None)
        if type(photo_src) is str:
            if "/clasificados/usados" in photo_src:
                # print(photo_src)
                photos_list.append(PHOTO_URL.format(photo_src))

    print(photos_list)

    for p_url in photos_list:
        r = requests.get(p_url)
        img = Image.open(BytesIO(r.content))
        
        print(img)

    return photos_list
        
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

    data = {"cardetails": [], "info": [], "equipamiento": [], "detalles": [], "photos": []}

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

    df = df.dropna(axis="columns", how="all")

    local_columns = [
        'Cilindrada', 'Estilo', 'Combustible', 'Transmisión', 'Estado', 'Kilometraje', 
        'Placa', 'Color exterior', 'Color interior', 'de puertas', 'Ya pagó impuestos', 
        'Precio negociable', 'Se recibe vehículo', 'Provincia', 'Fecha de ingreso', 'Modelo', 
        'Año', 'Precio Colones', 'Precio Dolares', 'link']

    # if not present then created column with nan
    for lc in local_columns:
        if lc not in df.columns:
            df[lc] = np.nan

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
            # break
        except Exception as exc:
            print(exc)

    # concat every dfs

    try:
        df = pd.concat(df_list)
    except Exception as exc:
        print(exc)
        return pd.DataFrame()


    df["Fecha de ingreso"] = df["Fecha de ingreso"].apply(parse_date)
    df["Fecha de hoy"] = date.today()
    df["Dias desde publicado"] = (df["Fecha de hoy"] - df["Fecha de ingreso"]).dt.days

    # print(df[["Fecha de ingreso","Fecha de hoy", "Dias desde publicado"]])

    



    df["Cilindrada"] = extract_str_to_int(df["Cilindrada"])
    df["Kilometraje"] = extract_str_to_int(df["Kilometraje"])
    df["Precio Colones"] = extract_str_to_int(df["Precio Colones"])
    df["Precio Dolares"] = extract_str_to_int(df["Precio Dolares"])

    df["Año"] = extract_str_to_int(df["Año"])

    current_year = date.today().year 
    df["KM por año"] = df["Kilometraje"]/(current_year - df["Año"])

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
        {"brand": 34, "modelstr": "jimny"},

        {"brand": 35, "modelstr": "hilux"},
        {"brand": 35, "modelstr": "rav4"},
        {"brand": 35, "modelstr": "yaris"},
        {"brand": 35, "modelstr": "fortuner"},
        {"brand": 35, "modelstr": "corolla"},
        {"brand": 35, "modelstr": "prado"},
        {"brand": 35, "modelstr": "echo"},
        
        
        
        {"brand": 16, "modelstr": "tucson"},
        {"brand": 16, "modelstr": "santa fe"},
        {"brand": 16, "modelstr": "creta"},
        {"brand": 16, "modelstr": "elantra"},
        {"brand": 16, "modelstr": "accent"},
        {"brand": 16, "modelstr": "i10"},
        {"brand": 16, "modelstr": "verna"},
        

        {"brand": 15, "modelstr": "crv"},

        {"brand": 26, "modelstr": "kicks"},
        {"brand": 26, "modelstr": "qashqai"},
        {"brand": 26, "modelstr": "xtrail"},


        {"brand": 17, "modelstr": "dmax"},

        {"brand": 19, "modelstr": "sportage"},
        {"brand": 19, "modelstr": "sorento"},
        
        {"brand": 23, "modelstr": "bt"},
        {"brand": 23, "modelstr": "cx"},
        {"brand": 23, "modelstr": "miata"},
          

        {"brand": 18, "modelstr": "wrangler"},
        {"brand": 18, "modelstr": "cherokee"},
        {"brand": 18, "modelstr": "gladiator"},
        {"brand": 18, "modelstr": "rubicon"},
        
        {"brand": 25, "modelstr": "L200"}, 
        {"brand": 25, "modelstr": "montero"}, 
        {"brand": 25, "modelstr": "outlander"}, 
        {"brand": 25, "modelstr": "asx"}, 
        {"brand": 25, "modelstr": "eclipse"}, 
        
        {"brand": 5, "modelstr": "z4"}, 
        {"brand": 5, "modelstr": "x"}, 
        
        {"brand": 36, "modelstr": "amarok"}, 
        {"brand": 36, "modelstr": "tiguan"}, 
        {"brand": 36, "modelstr": "taos"}, 
        
        


        
    ]

    df_cars = []
    car_brands = read_car_brands()
    for e, custom_query in enumerate(custom_brand_query):


        brand = car_brands.get(str(custom_query["brand"]), "Sin identificar")
        car_model = custom_query.get("modelstr", "Sin identificar")

        print(f"Iteration {e+1} of {len(custom_brand_query)}. Brand {brand} model: {car_model}")

        formdata_local = formdata
        formdata_local.update(custom_query)

        car_links = get_posted_cars_links(formdata_local)

        df = iter_car_links(car_links)

        df["Marca"] = brand.title()
        df["Modelstr"] = car_model.title()

        df_cars.append(df)

    if df_cars :
    
        df_cars = pd.concat(df_cars)
        df_cars = df_cars.drop_duplicates(subset="link")
        
        if "Precio Colones" in df_cars.columns:
            df_cars = df_cars.sort_values("Precio Colones")

        
        df_cars = df_cars.reset_index(drop=True)

        save_to_file(df_cars)
    else:

        print("WTF no cars")


if __name__ == "__main__":
    main()
