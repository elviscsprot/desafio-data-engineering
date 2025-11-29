import sqlite3
import pandas as pd
from datetime import datetime


def carregar_csvs():
    """Carrega os CSVs das bases local e GCP"""

    # CSV local usa separador ; (ponto e vírgula)
    df_local = pd.read_csv('dados/application_record_local.csv', sep=';')

    # CSV GCP usa separador , (vírgula)
    df_gcp = pd.read_csv('dados/application_record_gcp.csv', sep=',')

    print(f'Base Local: {len(df_local)} registros')
    print(f'Base GCP: {len(df_gcp)} registros')

    return df_local, df_gcp


def criar_banco(conn):
    """Cria as tabelas no SQLite"""
    cursor = conn.cursor()

    # Tabela para base local
    cursor.execute('DROP TABLE IF EXISTS application_record_local')
    cursor.execute('DROP TABLE IF EXISTS application_record_gcp')
    cursor.execute('DROP TABLE IF EXISTS tb_inconsistencias_aplicacoes')

    conn.commit()


def inserir_dados(conn, df_local, df_gcp):
    """Insere os dados dos CSVs no banco"""

    df_local.to_sql('application_record_local', conn, if_exists='replace', index=False)
    df_gcp.to_sql('application_record_gcp', conn, if_exists='replace', index=False)

    print('Dados inseridos no banco SQLite')


def executar_validacao(conn, id_inicio, id_fim):
    """Executa a validação comparando as duas bases"""

    cursor = conn.cursor()
    data_validacao = datetime.now().strftime('%Y-%m-%d')
    data_processamento = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Criar tabela de inconsistências
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tb_inconsistencias_aplicacoes (
            tipo_inconsistencia TEXT,
            id_registro INTEGER,
            descricao TEXT,
            dados_local TEXT,
            dados_gcp TEXT,
            data_validacao TEXT,
            data_processamento TEXT
        )
    ''')

    # Limpar tabela anterior
    cursor.execute('DELETE FROM tb_inconsistencias_aplicacoes')

    # 1. Registros ausentes no GCP (existem no local mas não no GCP)
    cursor.execute(f'''
        INSERT INTO tb_inconsistencias_aplicacoes
        SELECT
            'AUSENTE_GCP' as tipo_inconsistencia,
            l.ID as id_registro,
            'Registro ID ' || l.ID || ' existe no local mas não foi encontrado no GCP' as descricao,
            l.CODE_GENDER || '|' || l.AMT_INCOME_TOTAL || '|' || l.NAME_INCOME_TYPE as dados_local,
            NULL as dados_gcp,
            '{data_validacao}' as data_validacao,
            '{data_processamento}' as data_processamento
        FROM application_record_local l
        LEFT JOIN application_record_gcp g ON l.ID = g.ID
        WHERE g.ID IS NULL
        AND l.ID BETWEEN {id_inicio} AND {id_fim}
    ''')

    # 2. Registros ausentes no Local (existem no GCP mas não no local)
    cursor.execute(f'''
        INSERT INTO tb_inconsistencias_aplicacoes
        SELECT
            'AUSENTE_LOCAL' as tipo_inconsistencia,
            g.ID as id_registro,
            'Registro ID ' || g.ID || ' existe no GCP mas não foi encontrado no local' as descricao,
            NULL as dados_local,
            g.CODE_GENDER || '|' || g.AMT_INCOME_TOTAL || '|' || g.NAME_INCOME_TYPE as dados_gcp,
            '{data_validacao}' as data_validacao,
            '{data_processamento}' as data_processamento
        FROM application_record_gcp g
        LEFT JOIN application_record_local l ON g.ID = l.ID
        WHERE l.ID IS NULL
        AND g.ID BETWEEN {id_inicio} AND {id_fim}
    ''')

    # 3. Divergências de valores (mesmo ID, valores diferentes)
    cursor.execute(f'''
        INSERT INTO tb_inconsistencias_aplicacoes
        SELECT
            'DIVERGENCIA_VALORES' as tipo_inconsistencia,
            l.ID as id_registro,
            'Registro ID ' || l.ID || ' possui valores divergentes entre local e GCP' as descricao,
            l.CODE_GENDER || '|' || l.AMT_INCOME_TOTAL || '|' || l.DAYS_BIRTH || '|' || COALESCE(l.OCCUPATION_TYPE, 'NULL') as dados_local,
            g.CODE_GENDER || '|' || g.AMT_INCOME_TOTAL || '|' || g.DAYS_BIRTH || '|' || COALESCE(g.OCCUPATION_TYPE, 'NULL') as dados_gcp,
            '{data_validacao}' as data_validacao,
            '{data_processamento}' as data_processamento
        FROM application_record_local l
        INNER JOIN application_record_gcp g ON l.ID = g.ID
        WHERE l.ID BETWEEN {id_inicio} AND {id_fim}
        AND (
            l.CODE_GENDER != g.CODE_GENDER
            OR l.AMT_INCOME_TOTAL != g.AMT_INCOME_TOTAL
            OR l.DAYS_BIRTH != g.DAYS_BIRTH
            OR COALESCE(l.OCCUPATION_TYPE, '') != COALESCE(g.OCCUPATION_TYPE, '')
            OR l.FLAG_OWN_CAR != g.FLAG_OWN_CAR
            OR l.FLAG_OWN_REALTY != g.FLAG_OWN_REALTY
        )
    ''')

    conn.commit()
    print('Validação executada com sucesso!')


def exibir_resultados(conn):
    """Exibe os resultados da validação"""

    cursor = conn.cursor()

    # Resumo por tipo
    print('\n' + '='*60)
    print('RESUMO DAS INCONSISTÊNCIAS')
    print('='*60)

    cursor.execute('''
        SELECT tipo_inconsistencia, COUNT(*) as total
        FROM tb_inconsistencias_aplicacoes
        GROUP BY tipo_inconsistencia
        ORDER BY total DESC
    ''')

    resultados = cursor.fetchall()

    if not resultados:
        print('Nenhuma inconsistência encontrada!')
    else:
        for tipo, total in resultados:
            print(f'{tipo}: {total} registros')

    # Detalhes das primeiras inconsistências
    print('\n' + '='*60)
    print('DETALHES (primeiros 10 registros)')
    print('='*60)

    cursor.execute('''
        SELECT tipo_inconsistencia, id_registro, descricao, dados_local, dados_gcp
        FROM tb_inconsistencias_aplicacoes
        LIMIT 10
    ''')

    detalhes = cursor.fetchall()

    for row in detalhes:
        print(f'\nTipo: {row[0]}')
        print(f'ID: {row[1]}')
        print(f'Descrição: {row[2]}')
        if row[3]:
            print(f'Dados Local: {row[3]}')
        if row[4]:
            print(f'Dados GCP: {row[4]}')
        print('-' * 40)


def main():
    print('='*60)
    print('VALIDAÇÃO DE DADOS - LOCAL vs GCP')
    print('='*60 + '\n')

    # Carregar CSVs
    df_local, df_gcp = carregar_csvs()

    # Conectar ao banco
    conn = sqlite3.connect('validacao.db')

    # Criar estrutura
    criar_banco(conn)

    # Inserir dados
    inserir_dados(conn, df_local, df_gcp)

    # Executar validação (usando faixa de IDs dos dados)
    id_inicio = 5008804
    id_fim = 5100000

    print(f'\nValidando registros de ID {id_inicio} até {id_fim}...')
    executar_validacao(conn, id_inicio, id_fim)

    # Exibir resultados
    exibir_resultados(conn)

    conn.close()
    print('\n\nArquivo validacao.db criado com os resultados!')


if __name__ == '__main__':
    main()
