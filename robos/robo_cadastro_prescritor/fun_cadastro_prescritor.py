from datetime import datetime
from idlelib.iomenu import encoding
from pathlib import *
import sys
import requests
import os
import logging
import pyodbc
from dotenv import load_dotenv
from idna.idnadata import scripts


def get_resource_path(relative_path):
    """Retorna o caminho correto para arquivos adicionais, dentro ou fora do executável."""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.abspath(relative_path)

def get_logger(nome_robo: str):
    log_dir = os.path.join("logs", f"logs_{nome_robo}")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("log_%Y-%m-%d.txt")
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(f"logger_{nome_robo}")  # Usa nome único
    logger.setLevel(logging.INFO)

    # Remove handlers antigos se existirem
    if logger.hasHandlers():
        logger.handlers.clear()

    # Cria novo FileHandler e StreamHandler
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger.info



log = get_logger("cadastro_de_prescritor")

load_dotenv()



central_db_config = {
    "server": os.getenv("CENTRAL_DB_SERVER"),
    "database": os.getenv("CENTRAL_DB_DATABASE"),
    "username": os.getenv("CENTRAL_DB_USER"),
    "password": os.getenv("CENTRAL_DB_PASS")
}

def conectar_central():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={central_db_config['server']};"
            f"DATABASE={central_db_config['database']};"
            f"UID={central_db_config['username']};"
            f"PWD={central_db_config['password']}"
        )
        return conn
    except Exception as e:
        log(f"Erro ao conectar ao banco central: {e}")
        return None




def cadastrar_prescritor(uf, crm, tipo_cr):
    mensagem = ""
    conn = None
    cursor = None

    try:
        if tipo_cr != "CRM":
            mensagem = f"Os cadastros de {tipo_cr} precisam ser feitos manualmente, chamado encaminhado para análise"
            log(f"Os cadastros de {tipo_cr} precisam ser feitos manualmente, chamado encaminhado para análise")
            return

        conn = conectar_central()
        if not conn:
            mensagem = "Falha na conexão com o banco central."
            log(mensagem)
            return mensagem

        cursor = conn.cursor()

        dir_atual = Path(__file__).parent
        caminho_arquivo_crm = get_resource_path(f"robos/robo_cadastro_prescritor/CRM_INFO/{uf.upper()}.TXT")


        if not Path(caminho_arquivo_crm).exists():
            mensagem = f"O estado '{uf.upper()}' não encontrado. Verifique a sigla digitada."
            log(f"O estado '{uf.upper()}' não encontrado. Verifique a sigla digitada.")
            return

        nome_medico = None
        with open(caminho_arquivo_crm, 'r', encoding='utf-8') as file:
            for linha in file:
                campos = linha.strip().split("!")
                if len(campos) >= 3 and campos[0] == str(crm):
                    nome_medico = campos[2].upper()

                    if campos[4] != 'Ativo':
                        mensagem = f"Médico com CRM {crm}, Não Ativo na UF {(uf.upper())}."
                        log(f"Médico com CRM {crm}, Não Ativo na UF {(uf.upper())}.")
                        return
                    break

        if not nome_medico:
            mensagem =  f"Médico com CRM {crm}, Não encontrado na base de dados {(uf.upper())}."
            log(f"Médico com CRM {crm}, Não encontrado na base de dados {(uf.upper())}.")
            return




        cursor.execute("SELECT COUNT(*) FROM MEDICOS WHERE CR = ? AND UF = ?", (crm, uf.upper()))
        resultado = cursor.fetchone()

        if resultado[0] > 0:
            cursor.execute("update medicos set NOME = ? WHERE CR = ? AND UF = ?",
                           (nome_medico, crm, uf.upper()))
            conn.commit()
            mensagem = f"Médico: {nome_medico} CRM: {crm}, JÁ CADASTRADO NA UF {uf.upper()}."
            log(f"Médico: {nome_medico} CRM: {crm}, JÁ CADASTRADO NA UF {uf.upper()}.")
            return

        script_crm = """INSERT INTO medicos (
        NOME,
        CODIGO_CR,
        CR,
        UF
        )
        VALUES (?, 1, ?, ?);"""
        cursor.execute(script_crm, (nome_medico, crm, uf.upper()))
        conn.commit()

        mensagem =  f"Médico(a) {nome_medico}, Cadastrado com sucesso!"
        log(f"Médico(a) {nome_medico}, Cadastrado com sucesso!")
        return


    except Exception as e:
        log(f"erro ao conectar ao banco central: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return mensagem, tipo_cr













def interagir_chamado(cod_chamado, token, mensagem, tipo_cr):


    descricao = f"{mensagem}\n"
    if tipo_cr != "CRM":
        cod_status = "0000006"
    else:
        cod_status = "0000002"


    data_interacao = datetime.now().strftime("%d-%m-%Y")
    url = "https://api.desk.ms/ChamadosSuporte/interagir"

    payload = {
        "Chave": cod_chamado,
        "TChamado": {
            "CodFormaAtendimento": "1",
            "CodStatus": cod_status,
            "CodAprovador": [""],
            "TransferirOperador": "",
            "TransferirGrupo": "",
            "CodTerceiros": "",
            "Protocolo": "",
            "Descricao": descricao,
            "CodAgendamento": "",
            "DataAgendamento": "",
            "HoraAgendamento": "",
            "CodCausa": "000467",
            "CodOperador": "249",
            "CodGrupo": "",
            "EnviarEmail": "S",
            "EnvBase": "N",
            "CodFPMsg": "",
            "DataInteracao": data_interacao,
            "HoraInicial": "",
            "HoraFinal": "",
            "SMS": "",
            "ObservacaoInterna": "",
            "PrimeiroAtendimento": "S",
            "SegundoAtendimento": "N"
        },
        "TIc": {
            "Chave": {
                "278": "on",
                "280": "on"
            }
        }
    }

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    try:
        response = requests.put(url, json=payload, headers=headers)

        if response.status_code == 200:
            if cod_status == "0000006":
                log(f"Chamado {cod_chamado} encaminhado para análise. \n")

            if cod_status == "0000002":
                log(f"Chamado {cod_chamado} encerrado com sucesso! \n")
        else:
            log(f"Erro ao interagir no chamado. Código: {response.status_code}")
            log("Resposta da API:")
            log(response.text)
            try:
                erro_json = response.json()
                log(f"Detalhes do erro: {erro_json}")
            except ValueError:
                log("Não foi possível converter a resposta da API para JSON.")

    except requests.exceptions.RequestException as e:
        log(f"Erro ao conectar com a API: {e}")
