import csv
import psycopg2
from datetime import datetime

#Connect to the Database
conn = psycopg2.connect("host=localhost dbname=rfb user=postgres password=123456")
# conn = psycopg2.connect("host=192.168.22.29 dbname=rfb user=root password=123456")

cur = conn.cursor()
try:
    #START Import 'CSV Estabelecimentos'
    with open('C:/Users/wrocha/Desktop/rfb/Estabelecimento/18_06_2021/K3241K03200Y9D10612ESTABELE.CSV', 'r', encoding="ISO-8859-1") as f:
        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:

            #START formatting the dates
            data_situ = None
            if row[6] != '0' and row[6] != '':
             data_situ = datetime.strptime(row[6], '%Y%m%d')

            data_atividade = None
            if row[10] != '0' and row[10] != '':
             data_atividade = datetime.strptime(row[10], '%Y%m%d')


            data_sit_especial = None
            if row[29] != '0' and row[29] != '':
             data_sit_especial = datetime.strptime(row[29], '%Y%m%d')
            #END formatting the dates

            #Querie for INSERT
            cur.execute(
             "INSERT INTO empresas(cnpj, matriz_filial, nome_fantasia, situacao, data_situacao, motivo_situacao, "
             "nm_cidade_exterior, cod_pais, data_inicio_ativ, cnae_fiscal, tipo_logradouro, logradouro, numero, "
             "complemento, bairro, cep, uf, cod_municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, num_fax,"
             "email, sit_especial, data_sit_especial)"
             "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
             " %s, %s, %s)",
             (row[0] + row[1].zfill(4) + row[2].zfill(2), row[3], row[4], row[5], data_situ,
              row[7], row[8], row[9], data_atividade, row[11], row[13], row[14], row[15], row[16],
              row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27],
              row[28], data_sit_especial)
            )

    # # #Importa CSV Empresas para o banco
    with open('C:/Users/wrocha/Desktop/rfb/Empresas/18-06-2021/K3241K03200Y9D10612EMPRE.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader(f, delimiter=';')
        for row in reader:

            sql = """ UPDATE empresas
                            SET razao_social = %s, cod_nat_juridica = %s, qualif_resp = %s, capital_social = %s,
                            porte = %s
                            WHERE razao_social IS NULL
                            AND cnpj LIKE %s """

            cur.execute(sql, (row[1], row[2], row[3], row[4], None if row[5] == '' else row[5], row[0]+'%'))

    # # #Importa CSV com dados sobre o Simples Nacional
    with open('C:/Users/wrocha/Desktop/rfb/Dados Simples Nacional/FK03200$WSIMPLES.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:

            data_opc_simples = None
            if row[2] != '00000000' and row[2] != '':
                data_opc_simples = datetime.strptime(row[2], '%Y%m%d')

            data_exc_simples = None
            if row[3] != '00000000' and row[3] != '':
                data_exc_simples = datetime.strptime(row[3], '%Y%m%d')

            sql = """ UPDATE empresas
                            SET opc_simples = %s, data_opc_simples = %s, data_exc_simples = %s, opc_mei = %s
                            WHERE cnpj LIKE %s"""

            cur.execute(sql, (row[1], data_opc_simples, data_exc_simples, row[4], row[0]+'%'))

    # # Importa CSV Socios
    with open('C:/Users/wrocha/Desktop/rfb/Socios/K3241K03200Y4D10612SOCIO.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:
            sql = """ INSERT INTO socios(cnpj, tipo_socio, nome_socio, cnpj_cpf_socio, cod_qualificacao,data_entrada,
            cod_pais_ext, cpf_repres, nome_repres, cod_qualif_repres, faixa_etaria)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            cur.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))

    # # Importa CSV com dados dos municípios
    with open('C:/Users/wrocha/Desktop/rfb/Cidades/FK03200$ZD10612MUNIC.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:

            sql = """ INSERT INTO municipios(cod_municipio, descricao) VALUES (%s, %s)"""

            cur.execute(sql, (row[0], row[1]))

    # # Importa CSV com dados da natureza juridica
    with open('C:/Users/wrocha/Desktop/rfb/Natureza/FK03200$ZD10612NATJU.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:
            sql = """ INSERT INTO natureza_juridica(cod_nat_juridica, descricao) VALUES (%s, %s)"""

            cur.execute(sql, (row[0], row[1]))

    # # Importa CSV com dados dos municípios
    with open('C:/Users/wrocha/Desktop/rfb/Atributo CNAE/FK03200$ZD10612CNAE.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:
            sql = """ INSERT INTO cnaes(cod_cnae, descricao) VALUES (%s, %s)"""

            cur.execute(sql, (row[0], row[1]))

    # Importa CSV com dados dos municípios
    with open('C:/Users/wrocha/Desktop/rfb/Pais/FK03200$ZD10612PAIS.CSV', 'r', encoding="ISO-8859-1") as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:
            sql = """ INSERT INTO pais(cod_pais, descricao) VALUES (%s, %s)"""

            cur.execute(sql, (row[0], row[1]))

    with open('C:/Users/wrocha/Desktop/rfb/cansado/estado_202108021759.csv') as f:

        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=';')
        for row in reader:
            sql = """ INSERT INTO estado(id, uf)
            VALUES (%s, %s)"""

            cur.execute(sql, (row[0], row[1]))

    with open('C:/Users/wrocha/Desktop/rfb/cansado/TABMUN-SIAFI.csv') as f:

        reader = csv.reader(f, delimiter=';')
        for row in reader:
            sql = """ UPDATE municipios
                             SET uf = %s
                             WHERE cod_municipio = %s"""

            print(row[3])

            cur.execute(sql, (row[3], row[0]))

    #Commit the statement
    conn.commit()
except psycopg2.DatabaseError as error:
    print(error)
finally:
    if conn is not None:
        print('PRONTO!')
        #close the database communication
        conn.close()