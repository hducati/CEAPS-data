# Como funciona?

1. Duas pastas serão criadas, ceaps - csv files(contendo os arquivos csv) e ceaps - txt files(contendo os txt's).
2. Os arquivos txts criados terão 3 informações, sendo: Nome do senador, mês e o valor total reembolsado do respectivo mês.
3. Simples assim =).

# __Como rodar__?

1. Certifique-se de ter a versão 3 do python (`python --version` para verificar a versão).
2. Instale as dependências do projeto com: `pip install -r requirements.txt`
3. Após a instalação, abra novamente o prompt de comando(Windows + R).
4. Vá até a pasta em que o projeto foi baixado com o comando `cd`
5. Já dentro da pasta, digite o comando `scrapy crawl fetch_values`
6. Automaticamente serão geradas duas pastas, uma contendo os arquivos baixados(.csv) e outro contendo os txts criados.

### Caso naõ funcione:

Após ter instalado os requirements, caso não consiga rodar o spider, faça o seguinte:

1. Vá até a pasta onde foi criada a .venv.
2. Ative a .venv com `Scripts\Activate.bat`.
3. Instale as dependências novamente e veja se resolva.
