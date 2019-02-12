# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from os import listdir, mkdir, getcwd
from os.path import join, isfile, exists


class FetchValuesSpider(scrapy.Spider):
    name = 'fetch_values'
    # allowed_domains = ['www12.senado.leg.br']
    start_urls = ['https://www12.senado.leg.br/transparencia/dados-abertos-transparencia/dados-abertos-ceaps']

    def parse(self, response):
        """
        Primeiramente, vai verificar se existe a pasta chamada
        'CEAPS - Data', caso não exista, ela será imediatamente criada, e todo o contéudo
        baixado será salvado dentro dela.
        """
        path = join(getcwd(), 'CEAPS - data')
        if not exists(path):
            mkdir(path)
            self.logger.info('Created a new directory %s' % path)
        else:
            self.logger.info('Directory already exists %s' % path)
        filename = response.css("#parent-fieldname-text p a::text").extract()
        filename.reverse()
        csv_files = [csv for csv in listdir(path) if ('.csv' in csv[-4:]) and (isfile(join(path, csv)))]
        if csv_files:
            for f in filename:
                f = f.replace(" ", "").split('-')[-1] + '.csv'
                if f in csv_files:
                    self.logger.info('File %s already downloaded' % f)
                else:
                    yield from self.make_request(f)
        else:
            for f in filename:
                f = f.replace(" ", "").split('-')[-1] + '.csv'
                yield from self.make_request(f)

    def make_request(self, f):
        """
        Faz uma requisição para baixar o arquivo .csv

        Arguments:
            f {[string]} -- [nome do arquivo]
        """
        url = 'http://www.senado.gov.br/transparencia/LAI/verba/{}'.format(f)
        yield scrapy.Request(url=url, callback=self.save_csv_file)

    def save_csv_file(self, response):
        # save csv file
        name = response.url.split('/')[-1]
        path = join(getcwd(), "CEAPS - data", name)
        self.logger.info('Saving CSV as %s...' % name)
        with open(path, 'wb') as f:
            f.write(response.body)
            self.logger.info('Successful')
            self.treating_csv_files(path)

    def treating_csv_files(self, path):
        """
        Todos os arquivos csv serão lidos e agrupados, respectivamente,
        pelo nome do senador e o mês, e por fim, será calculado o gasto
        de cada senador por mês.

        Arguments:
            path {[string]} -- [caminho para a pasta com o arquivo .csv]
        """

        # show all columns and rows
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        f = path.replace('.csv', '.txt')
        self.logger.info('Reading %s file...' % path.split("\\")[-1])
        df = pd.read_csv(path, header=1, encoding="ISO-8859-1", sep=";")
        df['VALOR_REEMBOLSADO'] = df['VALOR_REEMBOLSADO'].apply(lambda x: float(str(x).split()[0].replace(',', '.')))
        df2 = df.groupby(['SENADOR', 'MES'])['VALOR_REEMBOLSADO'].agg('sum')
        self.create_file(df2, f)

    def create_file(self, df2, f):
        """
        Vai verificar se existe a pasta chamada CEAPS - new data,
        caso não exista, uma será criada com o respectivo nome.
        Após verificar a existência da pasta, vai gerar um arquivo .txt
        dentro desta mesma pasta.

        O arquivo .txt contém:
        -nome do senador
        -mês em que ocorreu o reembolso
        -valor total dos reembolsos que ocorreram no respectivo mês de cada senador.
        
        Arguments:
            df2 {[DataFrame]} -- [senador e mês agrupados, e a soma total dos reembolsos do mês]
            f {[string]} -- [nome do arquivo .txt a ser criado]
        """

        path = join(getcwd(), 'CEAPS - new data')
        if not exists(path):
            mkdir(path)
        self.logger.info('Generating {} file in {}'.format(f.split("\\")[-1], path))
        df2.reset_index().to_csv(
            join(path, f.split("\\")[-1]), index=False, header=True,
            decimal=',', sep=';', float_format='%.2f', encoding='utf-8')
