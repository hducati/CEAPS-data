# -*- coding: utf-8 -*-
import scrapy
from os import listdir
from os.path import join, isfile

class FetchValuesSpider(scrapy.Spider):
    name = 'fetch_values'
    allowed_domains = ['www12.senado.leg.br']
    start_urls = ['https://www12.senado.leg.br/transparencia/dados-abertos-transparencia/dados-abertos-ceaps']

    def parse(self, response):
        path = "C:/Users/henrique.miranda/Downloads"
        filename = response.css("#parent-fieldname-text p a::text").extract()
        print(filename)
        files = [f for f in listdir(path) if isfile(join(path, f))]
        csv_files = [csv for csv in files if '.csv' in csv[-4:]]
        for f in filename:
            for csv in csv_files:
                if csv in f.split("/")[-1]:
                    self.logger.info('File %s already downloaded' % csv)
                else:
                    name = f.replace(" ", "").split('-')[-1]
                    url = 'http://www.senado.gov.br/transparencia/LAI/verba/{}.csv'.format(name)
                    print(name)
                    yield scrapy.Request(url=url, callback=self.save_csv_file)

    def save_csv_file(self, response):
        name = response.url.split('/')[-1]
        self.logger.info('Saving CSV as %s ' % name)
        with open(name, 'wb') as f:
            f.write(response.body)