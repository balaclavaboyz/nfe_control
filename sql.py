import sqlite3
from dataclasses import dataclass


@dataclass
class Sql:

    def create_tables(self, con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute('''
          CREATE TABLE IF NOT EXISTS "cliente" (
              "bairro"	TEXT,
              "cep"	TEXT,
              "cnpj/cpf"	INTEGER,
              "data_da_entrada/saida"	TEXT,
              "emissao"	TEXT,
              "municipio"	TEXT,
              "nome"	TEXT,
              "uf"	TEXT,
              PRIMARY KEY("cnpj/cpf")
          );
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS "header" (
            "chave_acesso"	TEXT,
            "cnpj"	INTEGER,
            "ie"	INTEGER,
            "natureza"	TEXT,
            "protocolo"	INTEGER,
            "id_header"	INTEGER NOT NULL,
            "fk_cnpj/cpf"	INTEGER,
            FOREIGN KEY("fk_cnpj/cpf") REFERENCES "cliente"("cnpj/cpf") on delete cascade,
            PRIMARY KEY("id_header" AUTOINCREMENT)
        );
        ''')
        cur.execute('''
        CREATE TABLE "prod" (
            "cfop"	INTEGER,
            "codigo"	TEXT,
            "csosn"	INTEGER,
            "descricao"	TEXT,
            "fk_header"	INTEGER NOT NULL,
            "ncm/sh"	INTEGER,
            "qnt"	REAL,
            "unid"	INTEGER,
            "vlrunit"	REAL,
            "id"	INTEGER,
            FOREIGN KEY("fk_header") REFERENCES "header"("id_header") on delete cascade
        );
        ''')
        con.commit()

    def save_profile(self, nfe, con: sqlite3.Connection):
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
        con.commit()

    def save_prods(self, res, con, id_header):
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
        con.commit()

    def save_header(self, res, con):
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
        con.commit()
        id_header = cur.fetchone()[0]
        return id_header

    def delete_cliente(self, con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute('delete from cliente')
        con.commit()

    def delete_header(self, con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute('delete from header')
        con.commit()

    def delete_prod(self, con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute('delete from prod')
        con.commit()
