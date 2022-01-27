import requests
from bs4 import BeautifulSoup as bs
import json
import pandas as pd

# Read in default attribute list
with open('restaurant_attributes.txt', 'r') as f:
    attributes = [line.strip() for line in f]

# Main function to scrape restaurant meta data
def restaurant_metadata(shop_id='494380', full =False, include=attributes, return_df=True):
    session = requests.Session()
    session.headers.update({'user-agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Mobile Safari/537.36'})
    if type(shop_id) is str:
        try:
            url = f'https://www.openrice.com/en/hongkong/restaurant/{shop_id}'
            resp = session.get(url)
            if resp.status_code == 404:
                raise ValueError('Invalid shop id.')
                return None
            soup = bs(resp.text, features="lxml")
            api_string = get_api_string(soup)
            resp = session.get(f'https://www.openrice.com/api/pois?uilang=en&{api_string}')
            soup = bs(resp.text)
            j_data = json.loads(resp.text)
            results = j_data['searchResult']['paginationResult']['results']
            matching_ix = [i for i, dict_ in enumerate(results) if dict_['poiId']==int(shop_id)][0]
            result = results[matching_ix]
            if full:
                return result
            else:
                return trim_json(result, attributes)
        except:
            print('Error at: ')
    elif type(shop_id) is list:
        output = []
        errors = []
        for shop in shop_id:
            try:
                url = f'https://www.openrice.com/en/hongkong/restaurant/{shop}'
                resp = session.get(url)
                if resp.status_code == 404:
                    raise ValueError('Invalid shop id.')
                    return None
                soup = bs(resp.text, features="lxml")
                api_string = get_api_string(soup)
                resp = session.get(f'https://www.openrice.com/api/pois?uilang=en&{api_string}')
                soup = bs(resp.text, features="lxml")
                j_data = json.loads(resp.text)
                results = j_data['searchResult']['paginationResult']['results']
                matching_ix = [i for i, dict_ in enumerate(results) if dict_['poiId']==int(shop)][0]
                result = results[matching_ix]
                if full:
                    output.append(result)
                else:
                    output.append(trim_json(result, attributes))
            except:
                errors.append(shop)
        if return_df:
            print(f'Error at: {errors}') if len(errors) > 0 else None
            return pd.DataFrame.from_dict(output)
        print(f'Error at: {errors}') if len(errors) > 0 else None
        return output


# Since the API does not support direct query of restaurant id, we'll have to query the corresponding restaurant by various conditions
def get_api_string(soup):
    api_string = ''
    conditions = json.loads(soup.find_all('script')[0]['data-target'])
    for key in conditions:
        for item in conditions[key]:
            api_string += f'{key}id={item}&'
    return api_string

# To trim the json object so that only important data remains
def trim_json(result, attributes):
    output = {}
    for attr in attributes:
        try:
            if attr == 'categoriesUI':
                output[attr] = [cat['name'] for cat in result[attr]]
                continue
            if attr == 'coordinates':
                output[attr] = {'lng':result['mapLongitude'], 'lat':result['mapLatitude']} 
                continue
            if attr == 'poiHours' and len(result['poiHours']) > 1:
                output[attr] = (result[attr][-1]['period1Start'],result[attr][-1]['period1End']) 
                continue
            if attr == 'onlineBooking':
                output[attr] = 1 if 'tmBookingWidget' in result.keys() else 0
                continue
            if attr == 'district':
                output[attr] = result['district']['name']
                continue
            output[attr] = result[attr]
        except:
            pass
    return output