import pickle
import re
import os
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join
from collections import Counter
import json
from dataclasses import dataclass
from pprint import pprint
import numpy
import sqlite3
import pandas
import xmltodict
import xml.etree.ElementTree as ET

def parse_nfe_xml(x):
    list_res = []
    xml_dados = {"header": {}, "destinatario/remetente": {}, "prod": {}}

    h = ["cnpj", "chave_acesso", "natureza", "protocolo", "ie"]
    dr = [
        "nome",
        "cnpj/cpf",
        "emissao",
        "bairro",
        "cep",
        "data_da_entrada/saida",
        "municipio",
        "uf",
    ]
    dados_prod = [
        "id",
        "codigo",
        "descricao",
        "ncm/sh",
        "csosn",
        "cfop",
        "unid",
        "qnt",
        "vlrunit",
    ]
    for i in h:
        xml_dados["header"].update({i: ""})
    for i in dr:
        xml_dados["destinatario/remetente"].update({i: ""})
    for i in dados_prod:
        xml_dados["prod"].update({i: ""})
    if x.get('nfeProc', None) is None:
        return None
    h_input = [
        x["nfeProc"]["NFe"]["infNFe"]["emit"]["CNPJ"],
        x['nfeProc']["protNFe"]["infProt"]["chNFe"],
        x["nfeProc"]["NFe"]["infNFe"]["ide"]["natOp"],
        x["nfeProc"]["protNFe"]["infProt"]["nProt"],
        x["nfeProc"]["NFe"]["infNFe"]["emit"]["IE"],
    ]
    if x.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('dest', {}).get('CPF', None) is None:
        dr_input = [
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["xNome"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["CNPJ"],
            x["nfeProc"]["NFe"]["infNFe"]["ide"]["dhEmi"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["xBairro"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["CEP"],
            x["nfeProc"]["NFe"]["infNFe"]["ide"]["dhSaiEnt"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["xMun"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["UF"],
        ]
    else:
        dr_input = [
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["xNome"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["CPF"],
            x["nfeProc"]["NFe"]["infNFe"]["ide"]["dhEmi"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["xBairro"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["CEP"],
            x["nfeProc"]["NFe"]["infNFe"]["ide"]["dhSaiEnt"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["xMun"],
            x["nfeProc"]["NFe"]["infNFe"]["dest"]["enderDest"]["UF"],
        ]

    dados_prod_input = []
    prods: dict = x["nfeProc"]["NFe"]["infNFe"]["det"]
    if type(prods) is list:
        for i in prods:
            temp = [
                i["@nItem"],
                i["prod"]["cProd"],
                i["prod"]["xProd"],
                i["prod"]["NCM"],
                i["prod"]["CFOP"],
                i["prod"]["uCom"],
                i["prod"]["qCom"],
                i["prod"]["vUnCom"],
                i["prod"]["vProd"],
            ]
            dados_prod_input.append(temp)
    if type(prods) is dict:
        temp = [
            prods["@nItem"],
            prods["prod"]["cProd"],
            prods["prod"]["xProd"],
            prods["prod"]["NCM"],
            prods["prod"]["CFOP"],
            prods["prod"]["uCom"],
            prods["prod"]["qCom"],
            prods["prod"]["vUnCom"],
            prods["prod"]["vProd"],
        ]
        dados_prod_input.append(temp)

    i = 0
    j = 0
    while i < len(h_input) and j < len(h):
        xml_dados["header"][h[i]] = h_input[j]
        i += 1
        j += 1
    i = 0
    j = 0
    while i < len(dr_input) and j < len(dr):
        xml_dados["destinatario/remetente"][dr[j]] = dr_input[i]
        i += 1
        j += 1
    if type(dados_prod_input) == list:
        xml_dados["prod"].clear()
        for _ in range(len(dados_prod_input)):
            xml_dados["prod"].update({_: {}})
            for o in dados_prod:
                xml_dados["prod"][_].update({o: ""})
        for _ in range(len(dados_prod_input)):
            i = 0
            j = 0
            while i < len(dados_prod) and j < len(dados_prod_input[_]):
                xml_dados["prod"][_].update({dados_prod[j]: dados_prod_input[_][i]})
                # xml_dados['prod'][_][dados_prod[j]] = dados_prod_input[i]
                i += 1
                j += 1

    return xml_dados


def save_profile(nfe, con):
    # debuggin
    show_last_insert = False
    #
    cur = con.cursor()
    profile_id = nfe['destinatario/remetente']['cnpj/cpf']

    cur.execute('''
        select * from cliente where "cnpj/cpf" == ?
    ''', (profile_id,))
    if cur.fetchone() is None:
        cur.execute('''
            insert into cliente (bairro,cep,"cnpj/cpf","data_da_entrada/saida",emissao,municipio,nome,uf) values(?,?,?,?,?,?,?,?)
        ''', (nfe['destinatario/remetente']['bairro'],
              nfe['destinatario/remetente']['cep'],
              nfe['destinatario/remetente']['cnpj/cpf'],
              nfe['destinatario/remetente']['data_da_entrada/saida'],
              nfe['destinatario/remetente']['emissao'],
              nfe['destinatario/remetente']['municipio'],
              nfe['destinatario/remetente']['nome'],
              nfe['destinatario/remetente']['uf']))
        if show_last_insert:
            cur.execute('''
            select * from cliente order by "cnpj/cpf" desc limit 1
            ''')
            print(cur.fetchone())
    # print(profile_id)


def check_old_xml(filenames):
    if not os.path.exists(f'./known_files/know_files.pickle') or os.path.getsize(
            './known_files/know_files.pickle') == 0:
        file_names = []
        with open('./known_files/know_files.pickle', 'wb') as f:
            pickle.dump(file_names, f)

    with open('./known_files/know_files.pickle', 'rb') as f:
        to_be_added = []
        file_names = pickle.load(f)
        for x in filenames:
            if x not in file_names:
                to_be_added.append(x)
        if not to_be_added:
            return None
        return to_be_added


def save_header(res, con):
    header = res['header']
    cur = con.cursor()

    cur.execute('''
        insert into header(chave_acesso,
        cnpj,
        ie,
        natureza,
        protocolo,
        "fk_cnpj/cpf"
        ) values(?,?,?,?,?,?)
    
    ''', (header['chave_acesso'],
          header['cnpj'],
          header['ie'],
          header['natureza'],
          header['protocolo'],
          res['destinatario/remetente']['cnpj/cpf']))
    # cur.execute('select * from header')
    # print(cur.fetchone())

    cur.execute('''
    select "id_header" from header order by "id_header" desc limit 1
    ''')
    # print(cur.fetchone())
    id_header = cur.fetchone()[0]
    return id_header


def save_prods(res, con, id_header):
    prod = res['prod']
    cur = con.cursor()

    for i, v in prod.items():
        cur.execute('''insert into prod(
        cfop,
        codigo,
        csosn,
        descricao,
        fk_header,
        "ncm/sh",
        qnt,
        unid,
        vlrunit)values(?,?,?,?,?,?,?,?,?)''', (
            v['cfop'],
            v['codigo'],
            v['csosn'],
            v['descricao'],
            # fk header
            int(id_header),
            # v['id'],
            v['ncm/sh'],
            v['qnt'],
            v['unid'],
            v['vlrunit']
        ))

    # cur.execute('select * from prod')
    # print(cur.fetchall())


if __name__ == "__main__":
    # from cProfile import Profile
    # from pstats import SortKey, Stats

    # with Profile() as profile:
    #     con = sqlite3.connect("db.db")
    #     cur = con.cursor()

    #     mypath = "./xmls/"
    #     onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    #     list_parsed_xml = []
    #     for xml_file in onlyfiles:
    #         with open(f"{mypath}/{xml_file}") as f:
    #             list_parsed_xml.append(parse_nfe_xml(xmltodict.parse(f.read())))

    #     pprint(list_parsed_xml)
    #     (Stats(profile).strip_dirs().sort_stats(SortKey.CALLS).print_stats())
    # df = pandas.DataFrame()

    con = sqlite3.connect("db.db")
    cur = con.cursor()

    mypath = "./xmls/"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    list_parsed_xml = []

    list_to_be_added = check_old_xml(onlyfiles)
    if list_to_be_added is not None:
        # adding new entry to db
        desti = pandas.DataFrame()
        prod = pandas.DataFrame()
        for xml_file in list_to_be_added:
            with open(f"{mypath}/{xml_file}") as f:
                # list_parsed_xml.append(parse_nfe_xml(xmltodict.parse(f.read())))
                res = parse_nfe_xml(xmltodict.parse(f.read()))
                if res is None or res['destinatario/remetente']['nome'] == 'EBAZAR.COM.BR LTDA':
                    # case: nfe is cancelled or ebazer.com.br
                    continue

                save_profile(res, con)
                id_header = save_header(res, con)
                save_prods(res, con, id_header)
                con.commit()
                with open('./known_files/know_files.pickle', 'rb') as fff:
                    old = pickle.load(fff)
                    for i in list_to_be_added:
                        old.append(i)
                    with open('./known_files/know_files.pickle', 'wb') as ff:
                        pickle.dump(old, ff)

                exit()
                new_desti = pandas.DataFrame([res['destinatario/remetente']])
                desti = pandas.concat([desti, new_desti], ignore_index=True)

                for i in res['prod']:
                    new_prod = pandas.DataFrame([i])
                    prod = pandas.concat([prod, new_prod], ignore_index=True)

    desti = desti[desti.nome != 'EBAZAR.COM.BR LTDA']
    uf_count = desti.groupby(['uf']).agg(len).sort_values(['nome'], ascending=False)
    print(uf_count.columns)
    print(uf_count.index)

    # numero de venda por uf
    uf_count['municipio'].plot(kind='bar', subplots=True, sharex=True, sharey=True, title='Ocorrencia de vendas por UF')
    plt.show()

    print(prod)
    # quai prods

    # pprint(list_parsed_xml[0]['destinatario/remetente'])
    # pprint(list_parsed_xml[0]['header'])
    # pprint(list_parsed_xml[0]['prod'])
