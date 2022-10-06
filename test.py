from datetime import datetime

import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import re

req = Request("http://insideairbnb.com/get-the-data/")
html_page = urlopen(req)

soup = BeautifulSoup(html_page, "lxml")

links = []
for link in soup.findAll('a'):
    links.append(link.get('href'))

print(links)