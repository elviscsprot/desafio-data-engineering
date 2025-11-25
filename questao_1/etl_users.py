import requests
import sqlite3
import json


def criar_tabelas(conn):
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY,
            nome TEXT NOT NULL,
            email TEXT,
            idade INTEGER,
            genero TEXT,
            telefone TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS empresas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            nome_empresa TEXT,
            cargo TEXT,
            departamento TEXT,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
        )
    ''')
    
    conn.commit()


def buscar_usuarios():
    url = 'https://dummyjson.com/users'
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['users']


def inserir_dados(conn, usuarios):
    cursor = conn.cursor()
    
    for usuario in usuarios:
        cursor.execute('''
            INSERT OR REPLACE INTO usuarios (id, nome, email, idade, genero, telefone)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            usuario['id'],
            f"{usuario['firstName']} {usuario['lastName']}",
            usuario['email'],
            usuario['age'],
            usuario['gender'],
            usuario['phone']
        ))
        
        cursor.execute('''
            INSERT INTO empresas (usuario_id, nome_empresa, cargo, departamento)
            VALUES (?, ?, ?, ?)
        ''', (
            usuario['id'],
            usuario['company']['name'],
            usuario['company']['title'],
            usuario['company']['department']
        ))
    
    conn.commit()


def main():
    print('Iniciando ETL de usuários...')
    
    usuarios = buscar_usuarios()
    print(f'Obtidos {len(usuarios)} usuários da API')
    
    conn = sqlite3.connect('users.db')
    
    criar_tabelas(conn)
    print('Tabelas criadas')
    
    inserir_dados(conn, usuarios)
    print('Dados inseridos com sucesso')
    
    conn.close()
    print('ETL concluído!')


if __name__ == '__main__':
    main()

