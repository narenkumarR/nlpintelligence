'''
from geopy import geocoders
geocoder_web = geocoders.GoogleV3()
location = geocoder_web.geocode('300 E. Sonterra San Antonio, TX 78258 United States')
import pprint
pprint.pprint(location.raw)
#sample location from google
{u'address_components': [{u'long_name': u'300',
                          u'short_name': u'300',
                          u'types': [u'street_number']},
                         {u'long_name': u'East Sonterra Boulevard',
                          u'short_name': u'E Sonterra Blvd',
                          u'types': [u'route']},
                         {u'long_name': u'Far North Central',
                          u'short_name': u'Far North Central',
                          u'types': [u'neighborhood', u'political']},
                         {u'long_name': u'San Antonio',
                          u'short_name': u'San Antonio',
                          u'types': [u'locality', u'political']},
                         {u'long_name': u'Bexar County',
                          u'short_name': u'Bexar County',
                          u'types': [u'administrative_area_level_2',
                                     u'political']},
                         {u'long_name': u'Texas',
                          u'short_name': u'TX',
                          u'types': [u'administrative_area_level_1',
                                     u'political']},
                         {u'long_name': u'United States',
                          u'short_name': u'US',
                          u'types': [u'country', u'political']},
                         {u'long_name': u'78258',
                          u'short_name': u'78258',
                          u'types': [u'postal_code']}],
 u'formatted_address': u'300 E Sonterra Blvd, San Antonio, TX 78258, USA',
 u'geometry': {u'location': {u'lat': 29.61525989999999,
                             u'lng': -98.48878119999999},
               u'location_type': u'ROOFTOP',
               u'viewport': {u'northeast': {u'lat': 29.61660888029149,
                                            u'lng': -98.48743221970848},
                             u'southwest': {u'lat': 29.6139109197085,
                                            u'lng': -98.4901301802915}}},
 u'partial_match': True,
 u'place_id': u'ChIJF6nlTAFiXIYRnh0fTT5lw7w',
 u'types': [u'street_address']}


create table location_identifier (url text, location_raw text, country text, country_code text,state text, state_code text,county text,locality text);
'''