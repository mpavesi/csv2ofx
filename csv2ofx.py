# -*- coding: utf-8 -*-
"""Conversor de Extratos CSV para o formato OFX.

Este script é uma ferramenta de linha de comando para converter arquivos de extrato
bancário ou de cartão de crédito, do formato CSV, para o formato OFX (Open Financial
Exchange), que é compatível com a maioria dos softwares de gerenciamento financeiro
(como o MS Money, GnuCash, etc.).

Funcionalidades Principais:
  - Processa arquivos CSV com cabeçalho dinâmico, identificando as colunas
    de data, valor e descrição por meio de um mapeamento flexível.
  - Extrai automaticamente informações da conta (banco, agência, conta/cartão)
    do próprio conteúdo do CSV.
  - Diferencia extratos de conta bancária e cartão de crédito, ajustando a
    estrutura do OFX e a lógica de sinais dos valores.
  - Valida os arquivos de entrada e fornece mensagens de erro claras.
  - Gera arquivos OFX com formato de data e codificação de caracteres
    compatíveis com softwares mais antigos.

Exemplo de Uso (no terminal):
    # Processa um extrato de conta bancária (opção padrão)
    python csv2ofx.py extrato_banco.csv

    # Processa uma fatura de cartão de crédito
    python csv2ofx.py fatura_cartao.csv -c
"""

import argparse
import csv
import hashlib
import os
import sys
from datetime import datetime


def validar_extensao_csv(caminho):
    """Valida se o caminho fornecido termina com a extensão .csv.

    Esta função é usada pelo `argparse` para verificar o formato do nome do arquivo
    de entrada antes de o script começar o processamento.

    Args:
        caminho (str): O caminho do arquivo fornecido na linha de comando.

    Returns:
        str: O caminho do arquivo, se for válido.

    Raises:
        argparse.ArgumentTypeError: Se o caminho não terminar com .csv.
    """
    if not caminho.lower().endswith('.csv'):
        raise argparse.ArgumentTypeError(
            f"O arquivo '{caminho}' não é um arquivo .csv válido."
        )
    return caminho


def preprocessar_csv_corrigindo_linhas(caminho_arquivo):
    """Lê o arquivo CSV, separa o cabeçalho e corrige quebras de linha nos dados.

    Alguns sistemas geram arquivos CSV onde uma única transação pode ser
    quebrada em múltiplas linhas. Esta função identifica o cabeçalho, e depois
    itera sobre as linhas de dados, juntando linhas "quebradas" à sua linha "mãe".

    Args:
        caminho_arquivo (str): O caminho para o arquivo CSV de entrada.

    Returns:
        tuple[str, list[str]] | tuple[None, None]: Uma tupla contendo a string
        do cabeçalho e uma lista com as linhas de dados corrigidas. Retorna
        None, None se o arquivo não for encontrado ou estiver vazio.
    """
    try:
        # A codificação 'utf-8-sig' é usada para lidar com o BOM (Byte Order Mark)
        # que alguns editores (especialmente no Windows) adicionam no início do arquivo.
        with open(caminho_arquivo, 'r', encoding='utf-8-sig') as f:
            linhas = f.readlines()
        
        if not linhas:
            print("Erro: Arquivo CSV está vazio.")
            return None, None

        cabecalho = linhas[0].strip()
        linhas_de_dados = linhas[1:]
        
        linhas_corrigidas = []
        for linha in linhas_de_dados:
            linha_limpa = linha.strip()
            if not linha_limpa:
                continue

            # Uma linha é considerada "quebrada" se tiver menos de 2 delimitadores.
            if linha_limpa.count(';') < 2:
                if linhas_corrigidas:
                    # Anexa o conteúdo da linha quebrada à penúltima coluna
                    # (geralmente a descrição) da linha anterior.
                    partes = linhas_corrigidas[-1].split(';')
                    partes[-2] = partes[-2] + ' ' + linha_limpa.split(';')[0]
                    linhas_corrigidas[-1] = ';'.join(partes)
            else:
                linhas_corrigidas.append(linha_limpa)

        return cabecalho, linhas_corrigidas

    except FileNotFoundError:
        # A verificação principal de existência é feita antes, mas mantemos por segurança.
        return None, None


def analisar_transacoes(cabecalho_str, linhas_csv, tipo_conta):
    """Analisa as linhas de dados do CSV e as converte em uma estrutura de dados.

    Esta função utiliza o cabeçalho para mapear as colunas do CSV de forma flexível.
    Ela também extrai os dados da conta (banco, agência, etc.) da primeira linha
    de dados e processa cada transação.

    Args:
        cabecalho_str (str): A string do cabeçalho do CSV.
        linhas_csv (list[str]): Uma lista de strings, onde cada uma é uma linha de dados.
        tipo_conta (str): O tipo de conta ('banco' ou 'credito').

    Returns:
        tuple[list[dict], str | None, str | None]: Uma tupla contendo:
            - Uma lista de dicionários, onde cada um representa uma transação.
            - O ID do banco encontrado.
            - O ID da conta/cartão encontrado.
    """
    # Mapeia possíveis nomes de coluna no CSV para nomes internos padronizados.
    # Permite flexibilidade no arquivo de entrada.
    MAPEAMENTO_CAMPOS = {
        'data': ['data', 'date'],
        'descricao': ['histórico', 'historico', 'descrição', 'descricao', 'description', 'memo'],
        'valor': ['valor', 'montante', 'amount', 'value'],
        'id_transacao': ['id', 'id da transação', 'id_da_transacao', 'checknum'],
        'bank_id': ['banco', 'bankid', 'código do banco', 'codigo_banco'],
        'agencia': ['agência', 'agencia'],
        'acct_id': ['conta', 'acctid', 'número da conta', 'numero_da_conta', 'cartão', 'cartao']
    }

    # Preserva o cabeçalho original (com maiúsculas/minúsculas) para o leitor de CSV
    # e cria uma versão em minúsculas para a busca de correspondências.
    cabecalho_original = [h.strip() for h in cabecalho_str.split(';')]
    cabecalho_busca = [h.lower() for h in cabecalho_original]
    
    # Constrói um mapa de {campo_interno: nome_original_do_cabeçalho}
    mapa_final = {}
    for campo_interno, possiveis_nomes in MAPEAMENTO_CAMPOS.items():
        for nome_candidato in possiveis_nomes:
            if nome_candidato in cabecalho_busca:
                index = cabecalho_busca.index(nome_candidato)
                nome_original = cabecalho_original[index]
                mapa_final[campo_interno] = nome_original
                break
    
    # Valida se os campos essenciais (data, descrição, valor) foram encontrados
    campos_obrigatorios = ['data', 'descricao', 'valor']
    for campo in campos_obrigatorios:
        if campo not in mapa_final:
            print(f"Erro: Não foi possível encontrar uma coluna para '{campo}' no cabeçalho do CSV.")
            print(f"O cabeçalho deve conter uma das seguintes opções para '{campo}': {MAPEAMENTO_CAMPOS[campo]}")
            return [], None, None

    # Extrai os dados da conta (banco, agência, etc.) da primeira linha de dados.
    bank_id_encontrado, agencia_encontrada, conta_encontrada, acct_id_final = (None,) * 4

    if linhas_csv:
        primeira_linha_dados = linhas_csv[0].split(';')
        
        if 'bank_id' in mapa_final:
            try:
                index = cabecalho_original.index(mapa_final['bank_id'])
                bank_id_encontrado = primeira_linha_dados[index].strip()
            except IndexError: pass

        if 'agencia' in mapa_final:
            try:
                index = cabecalho_original.index(mapa_final['agencia'])
                agencia_encontrada = primeira_linha_dados[index].strip()
            except IndexError: pass

        if 'acct_id' in mapa_final:
            try:
                index = cabecalho_original.index(mapa_final['acct_id'])
                conta_encontrada = primeira_linha_dados[index].strip()
            except IndexError: pass

        # Concatena agência e conta, se ambas existirem.
        if agencia_encontrada and conta_encontrada:
            acct_id_final = f"{agencia_encontrada}-{conta_encontrada}"
        elif conta_encontrada:
            acct_id_final = conta_encontrada
        
    transacoes = []
    # Usa o DictReader para que cada linha seja um dicionário {cabeçalho: valor}
    leitor_csv = csv.DictReader(linhas_csv, fieldnames=cabecalho_original, delimiter=';')
    
    for i, linha in enumerate(leitor_csv):
        try:
            data_str = linha[mapa_final['data']]
            descricao = linha[mapa_final['descricao']].strip()
            valor_str = linha[mapa_final['valor']]
            id_transacao = linha.get(mapa_final.get('id_transacao'), '')

            # Processamento e limpeza dos dados
            descricao_limpa = ' '.join(descricao.replace('*', ' ').split())
            data_obj = datetime.strptime(data_str, '%d/%m/%Y')
            valor_float = float(valor_str.replace(',', '.'))
            
            # Para cartão de crédito, o OFX espera que despesas sejam negativas.
            if tipo_conta == 'credito':
                valor_float = -valor_float

            # Gera um ID de transação único e consistente
            fitid_hash = hashlib.md5(f"{data_str}{descricao}{valor_str}{i}".encode('utf-8')).hexdigest()

            transacoes.append({
                'data': data_obj,
                'descricao': descricao_limpa,
                'valor': valor_float,
                'id_transacao': id_transacao,
                'fitid': fitid_hash
            })
        except (ValueError, IndexError, KeyError) as e:
            print(f"Aviso: Ignorando linha mal formatada ou com dados ausentes: {linha}. Erro: {e}")
            continue

    # Ordena as transações por data, uma exigência de muitos importadores OFX.
    transacoes.sort(key=lambda t: t['data'])
    
    return transacoes, bank_id_encontrado, acct_id_final


def gerar_ofx(transacoes, caminho_saida, tipo_conta, bank_id=None, acct_id=None):
    """Gera o arquivo OFX final a partir dos dados processados.

    Esta função constrói a estrutura do arquivo OFX em blocos de texto (templates),
    preenchendo-os com os dados das transações e as informações da conta. A
    estrutura muda ligeiramente se for uma conta bancária ou um cartão de crédito.

    Args:
        transacoes (list[dict]): A lista de transações processadas.
        caminho_saida (str): O nome do arquivo .ofx a ser criado.
        tipo_conta (str): 'banco' ou 'credito'.
        bank_id (str | None): O ID do banco, extraído do CSV.
        acct_id (str | None): O ID da conta/cartão, extraído do CSV.
    """
    if not transacoes:
        print("Nenhuma transação para processar. O arquivo OFX não será gerado.")
        return

    # Formato de data simples (YYYYMMDD) para máxima compatibilidade com softwares antigos.
    data_inicio = transacoes[0]['data'].strftime('%Y%m%d')
    data_fim = transacoes[-1]['data'].strftime('%Y%m%d')
    data_servidor = transacoes[-1]['data'].strftime('%Y%m%d')

    # Escolhe a estrutura do cabeçalho da conta (banco vs. crédito)
    if tipo_conta == 'credito':
        acct_id_final = acct_id if acct_id is not None else 'XXXX-CARTAO-NAO-DEFINIDO'
        template_conta_header = f"""  <CREDITCARDMSGSRSV1>
    <CCSTMTTRNRS>
      <TRNUID>1</TRNUID>
      <STATUS>
        <CODE>0</CODE>
        <SEVERITY>INFO</SEVERITY>
      </STATUS>
      <CCSTMTRS>
        <CURDEF>BRL</CURDEF>
        <CCACCTFROM>
          <ACCTID>{acct_id_final}</ACCTID>
        </CCACCTFROM>"""
        template_conta_footer = """      </CCSTMTRS>
    </CCSTMTTRNRS>
  </CREDITCARDMSGSRSV1>"""
    else:  # tipo_conta == 'banco'
        bank_id_final = bank_id if bank_id is not None else '000'
        acct_id_final = acct_id if acct_id is not None else 'XXXX-CONTA-NAO-DEFINIDA'
        template_conta_header = f"""  <BANKMSGSRSV1>
    <STMTTRNRS>
      <TRNUID>1</TRNUID>
      <STATUS>
        <CODE>0</CODE>
        <SEVERITY>INFO</SEVERITY>
      </STATUS>
      <STMTRS>
        <CURDEF>BRL</CURDEF>
        <BANKACCTFROM>
          <BANKID>{bank_id_final}</BANKID>
          <ACCTID>{acct_id_final}</ACCTID>
          <ACCTTYPE>CHECKING</ACCTTYPE>
        </BANKACCTFROM>"""
        template_conta_footer = """      </STMTRS>
    </STMTTRNRS>
  </BANKMSGSRSV1>"""

    # Template do cabeçalho geral do OFX, com codificação para alta compatibilidade
    template_cabecalho_geral = f"""OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE

<OFX>
  <SIGNONMSGSRSV1>
    <SONRS>
      <STATUS>
        <CODE>0</CODE>
        <SEVERITY>INFO</SEVERITY>
      </STATUS>
      <DTSERVER>{data_servidor}</DTSERVER>
      <LANGUAGE>POR</LANGUAGE>
    </SONRS>
  </SIGNONMSGSRSV1>
"""
    # Template para uma única transação
    template_transacao = """          <STMTTRN>
            <TRNTYPE>{trntype}</TRNTYPE>
            <DTPOSTED>{dtposted}</DTPOSTED>
            <TRNAMT>{trnamt}</TRNAMT>
            <FITID>{fitid}</FITID>
            <CHECKNUM>{checknum}</CHECKNUM>
            <MEMO>{memo}</MEMO>
          </STMTTRN>
"""
    # Template para o rodapé geral do OFX
    template_rodape_geral = f"""        <LEDGERBAL>
          <BALAMT>0.00</BALAMT>
          <DTASOF>{data_fim}</DTASOF>
        </LEDGERBAL>
{template_conta_footer}
</OFX>
"""
    # Template para o cabeçalho da lista de transações
    template_lista_transacoes = f"""        <BANKTRANLIST>
          <DTSTART>{data_inicio}</DTSTART>
          <DTEND>{data_fim}</DTEND>\n"""

    # Escreve o arquivo final, juntando todos os blocos de texto
    # Usa 'cp1252' como encoding e quebras de linha do Windows ('\r\n') para compatibilidade
    with open(caminho_saida, 'w', encoding='cp1252', newline='\r\n') as f:
        f.write(template_cabecalho_geral)
        f.write(template_conta_header)
        f.write(template_lista_transacoes)
        for t in transacoes:
            valor_ofx = f"{t['valor']:.2f}".replace('.', ',')
            tipo_transacao = "CREDIT" if t['valor'] > 0 else "DEBIT"
            data_transacao = t['data'].strftime('%Y%m%d')
            f.write(template_transacao.format(
                trntype=tipo_transacao,
                dtposted=data_transacao,
                trnamt=valor_ofx,
                fitid=t['fitid'],
                checknum=t['id_transacao'],
                memo=t['descricao']
            ))
        f.write("        </BANKTRANLIST>\n")
        f.write(template_rodape_geral)
        
    print(f"Arquivo '{caminho_saida}' gerado com sucesso com {len(transacoes)} transações.")


if __name__ == "__main__":
    """Ponto de entrada principal para a execução do script via linha de comando."""
    
    # Configura o parser de argumentos para interpretar os comandos do usuário
    parser = argparse.ArgumentParser(
        description="Converte um arquivo CSV (com cabeçalho) para o formato OFX.",
        epilog="Exemplo de uso: python csv2ofx.py minha_fatura.csv -c"
    )
    
    # Argumento obrigatório: nome do arquivo de entrada
    parser.add_argument(
        'arquivo_entrada',
        type=validar_extensao_csv,
        help='Nome do arquivo de entrada (deve ser .csv com cabeçalho na primeira linha).'
    )
    
    # Grupo de argumentos opcionais e mutuamente exclusivos (-c ou -b)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-c', '--credito',
        action='store_const',
        dest='tipo_conta',
        const='credito',
        help='Processar como extrato de Cartão de Crédito (inverte o sinal dos valores de despesa).'
    )
    group.add_argument(
        '-b', '--banco',
        action='store_const',
        dest='tipo_conta',
        const='banco',
        help='Processar como extrato de Conta Bancária (mantém os sinais originais dos valores).'
    )
    
    # Interpreta os argumentos fornecidos pelo usuário
    args = parser.parse_args()
    
    # Lógica para definir o tipo de conta padrão como 'banco' se nada for especificado
    if args.tipo_conta is None:
        args.tipo_conta = 'banco'

    # Validação da existência do arquivo de entrada
    if not os.path.exists(args.arquivo_entrada):
        print(f"ERRO: O arquivo de entrada '{args.arquivo_entrada}' não foi encontrado.")
        sys.exit(1)

    # Define os nomes de arquivo de entrada e saída
    arquivo_csv_entrada = args.arquivo_entrada
    arquivo_ofx_saida = os.path.splitext(arquivo_csv_entrada)[0] + '.ofx'

    # Inicia o processo de conversão
    print(f"Arquivo de entrada: '{arquivo_csv_entrada}'")
    print(f"Arquivo de saída: '{arquivo_ofx_saida}'")
    print(f"Iniciando processo de conversão para o modo '{args.tipo_conta}'...")
    
    cabecalho, linhas_processadas = preprocessar_csv_corrigindo_linhas(arquivo_csv_entrada)
    
    if cabecalho and linhas_processadas:
        lista_transacoes, bank_id, acct_id = analisar_transacoes(
            cabecalho, linhas_processadas, args.tipo_conta
        )
        gerar_ofx(
            lista_transacoes, arquivo_ofx_saida, args.tipo_conta, bank_id, acct_id
        )
        
    print("Processo finalizado.")