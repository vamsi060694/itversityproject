import os
from os.path import basename
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import datetime as dt
import json
import logging

load_dotenv()

filters = os.getenv('REFERENCE').split(',')
folder_path = os.getenv('FOLDER_PATH')
data_path = os.path.join(folder_path + '//' + 'data_folder')


def all_years(data_path, field, sources, url):
    answer = os.getenv('ALL_YEARS')

    if answer == 'yes':
        try:
            for source in sources:
                req = requests.get(f'{url}/{source}')
                with open(basename(f'{data_path}//{field}//{source}'), 'wb') as file:
                    file.write(req.content)
        except Exception as e:
            logging.error(e)

    else:
        try:
            d = dt.date.today()
            for source in sources:
                if source.split('.')[-2][-4:] == str(d.year):
                    req = requests.get(f'{url}/{source}')
                    with open(basename(f'{data_path}//{field}//{source}'), 'wb') as file:
                        file.write(req.content)
        except Exception as e:
            logging.error(e)


def pdf_links(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        websites = []
        for link in soup.find_all('a'):
            if 'pdf' in link['href']:
                websites.append(link.get('href'))
        return websites
    except Exception as e:
        logging.error(e)


def creating_folder(data_path):
    try:
        if not os.path.isdir(data_path):
            os.mkdir('../../data_folder')
    except Exception as e:
        logging.error(e)


def creating_fields(data_path, field):
    try:
        if not os.path.isdir(data_path + '//' + field):
            os.mkdir(field)
        os.chdir(data_path + '//' + field)
        print(">>", os.getcwd())
    except Exception as e:
        logging.error(e)


def downloading_src_files(filters, data_path):
    for filter in filters:
        try:
            url = f'https://www.cnlopb.ca/wp-content/uploads/{filter}'

            sources = pdf_links(url)
            creating_folder(data_path)

            field = json.loads(os.getenv('FIELDS'))[filter]
            # field = fields[filter]
            os.chdir(data_path)

            creating_fields(data_path, field)
            all_years(data_path, field, sources, url)
            os.chdir(data_path)

        except Exception as e:
            logging.error(e)


downloading_src_files(filters, data_path)
