from time import sleep
AAAAAAA
BBBBBBB
from sqlalchemy import *
import requests
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging, logging.handlers
import os
from pecten_utils.Storage import Storage
from pecten_utils import twitter_analytics_helpers as tah
from pecten_utils.BigQueryLogsHandler import BigQueryLogsHandler
from pecten_utils.duplication_handler import DuplicationHandler
from pathlib import Path
from googletrans import Translator
import re
import dateutil.parser
def main(args):
    sys.path.insert(0, args.python_path)
    args.storage = Storage(args.google_key_path)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = BigQueryLogsHandler(args.storage,args)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    args.logger=logger
    args.parameters=get_storage_details(args)
    args.datasets = tah.get_dataset_names(args.environment)
    try:
        get_fund_data(args)
    except Exception as e:
        print(e)
        args.logger.error(str(e),extra={"script_type":"collection","operation":"collecting analyst rating from investing.com and store to BQ", "criticality": 5, "data_loss":"not retrievable"})

def get_latest_date(args):
    query_params = [('etf_name','STRING','monega')]
    query = """SELECT max(fund_holding_date) as latest_date FROM {}.{} where etf_name=@etf_name
                """.format(args.datasets[0],args.parameters["TABLE_STORAGE"])
    result = args.storage.get_bigquery_data(query=query, iterator_flag=False,params=query_params)
    if result:
        try:
            return result[0]["latest_date"]
        except Exception as e:
            return None


def get_storage_details(args):
    Table = "PARAM_ANALYST_COLLECTION"
    Column_list = ["TABLE_STORAGE"]
    Where = lambda x: x["SOURCE"] == "etf"
    try:
        parameters = tah.get_parameters(connection_string=args.param_connection_string, table=Table,
                                    column_list=Column_list, where=Where)
        return parameters
    except Exception as e:
        print(e)
        return None

def get_fund_data(args):
    translator = Translator()
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-extensions')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument("--incognito")
    driver  = webdriver.Chrome('/Users/abhi/Downloads/chromedriver 4',options=options)
    driver.maximize_window()
    print("visiting monega website")
    driver.get('https://www.monega.de/fonds/de0005321038')
    sleep(5)
    url="/html/body/div[1]/main/div[4]/div/div[3]/table[{}]/tbody"
    driver.execute_script("window.scrollTo(screen.width/2, 1800);")
    driver.find_element_by_xpath('//*[@data-tab-id="portfolio"]').click()
    sleep(5)
    temp_date=driver.find_element_by_xpath(url.split('table')[0]+'h2[1]').text
    temp_date=dateutil.parser.parse(str(re.findall('\(([^)]+)',temp_date)[0])).date()
    print(temp_date)
    lastest_date=get_latest_date(args)
    if(lastest_date is None):
        lastest_date=date(2000,1,1)
    to_insert=[]
   
    try:
        if(temp_date>lastest_date):
            table=driver.find_element_by_xpath(url.format(1))
            for row in table.find_elements_by_css_selector('tr'):
                temp={}
                temp['constituent_name']=row.find_elements_by_tag_name('td')[1].text.strip()
                temp['percentage_of_asset']=row.find_elements_by_tag_name('td')[2].text.replace('%','').replace(',','.').strip()
                to_insert.append(temp)
                print(temp)
    except Exception as e:
        print(e)
    
    temp_date=driver.find_element_by_xpath(url.split('table')[0]+'h2[2]').text
    temp_date=dateutil.parser.parse(str(re.findall('\(([^)]+)',temp_date)[0])).date()
    print(temp_date)
    lastest_date=get_latest_date(args)
    if(lastest_date is None):
        lastest_date=date(2000,1,1)
    try:
        if(temp_date>lastest_date):
            table=driver.find_element_by_xpath('//*[@id="fsl_Industriespiechart"]').text
            for i in table.split('\n'):
                temp={}
                temp['sector_name']= translator.translate(i.split(':')[0], src='de', dest="en").text.strip() 
                temp['percentage']=i.split(':')[1].replace('%','').replace(',','.').strip()
                print(temp)
                to_insert.append(temp)
    except Exception as e:
        print(e)
    
    table=driver.find_element_by_xpath(url.format(2))
    values=[]
    for row in table.find_elements_by_css_selector('tr'):
        values.append(row.find_elements_by_tag_name('td')[0].text.replace('%','').replace(',','.'))
    temp={}
    temp['volatility']=values[0].strip()
    temp['sharpe_ratio']=values[1].strip()
    temp['max_drawdown']=values[2].strip()
    temp['value_at_risk']=values[3].strip()
    print(temp)
    driver.close()
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    args.python_path = os.environ.get('PYTHON_PATH', '')
    args.google_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
    args.environment = os.environ.get('ENVIRONMENT', '')
    args.param_connection_string = os.environ.get('MYSQL_CONNECTION_STRING', '')
    args.bucket_name = os.environ.get("BUCKET_NAME", 'pecten-duplication')
    args.duplicates_log_table = os.environ.get("DUPLICATES_LOG_TABLE", 'duplicate_data_utils')
    args.invalid_log_table = os.environ.get("INVALID_LOG_TABLE", "invalid_data_utils")
    sys.path.insert(0, args.python_path)
    main(args)
