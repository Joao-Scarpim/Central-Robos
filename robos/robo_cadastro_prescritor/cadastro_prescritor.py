import requests
import re
from . import fun_cadastro_prescritor as fun
from dotenv import load_dotenv
import os


def run(ativo):
    log = fun.get_logger("cadastro_de_prescritor")
    load_dotenv()

    # === 1. Autenticação Desk.ms ===
    try:
        url_auth_desk = "https://api.desk.ms/Login/autenticar"
        headers_desk = {
            "Authorization": "30c55b0282a7962061dd41a654b6610d02635ddf",
            "JsonPath": "true"
        }
        payload_desk = {
            "PublicKey": "1bb099a1915916de10c9be05ff4d2cafed607e7f"
        }

        response_desk = requests.post(url_auth_desk, json=payload_desk, headers=headers_desk)
        response_desk.raise_for_status()
        token_desk = response_desk.json()["access_token"]
        log(f"✅ Autenticação Desk.ms realizada com sucesso!")

    except Exception as e:
        log(f"❌ Erro na autenticação Desk.ms: {e}")
        exit()

    # === 2. Autenticação DataHub ===
    try:
        url_auth_datahub = "https://datahub-api.nisseilabs.com.br/auth/token"
        payload_datahub = {
            "grant_type": "password",
            "username": os.getenv("DATAHUB_USERNAME"),
            "password": os.getenv("DATAHUB_PASSWORD")
        }
        headers_datahub = {"Content-Type": "application/x-www-form-urlencoded"}

        response_datahub = requests.post(url_auth_datahub, data=payload_datahub, headers=headers_datahub)
        response_datahub.raise_for_status()
        token_datahub = response_datahub.json()["access_token"]
        log("✅ Autenticação DataHub realizada com sucesso!")

    except Exception as e:
        log(f"❌ Erro na autenticação com DataHub: {e}")
        exit()

    # === 3. Listar Chamados no Desk.ms ===
    url_listar_chamados = "https://api.desk.ms/ChamadosSuporte/lista"
    headers_chamados = {
        "Authorization": f"{token_desk}"
    }

    payload_chamados = {
        "Pesquisa": "CSN - CADASTRO DE PRESCRITOR",
        "Tatual": "",
        "Ativo": ativo,
        "StatusSLA": "",
        "Colunas": {
            "Chave": "on", "CodChamado": "on", "NomeUsuario": "on",
            "Descricao": "on", "_126143": "on", "_126157": "on", "_126152": "on"
        },
        "Ordem": [
            {
                "Coluna": "Chave",
                "Direcao": "true"
            }
        ]
    }

    try:
        response_chamados = requests.post(url_listar_chamados, json=payload_chamados, headers=headers_chamados)
        response_chamados.raise_for_status()
        chamados = response_chamados.json()["root"]

        for chamado in chamados:
            descricao = chamado["Descricao"]
            cod_chamado = chamado["CodChamado"]
            cod_cr = chamado["_126143"].strip()
            uf_prescritor = chamado["_126157"]
            tipo_cr = chamado["_126152"]

            # === Extrair UF ===
            match_uf = re.search(r'\b[A-Z]{2}\b', uf_prescritor)
            if not match_uf:
                log(f"❌ UF inválida no chamado {cod_chamado}. Pulando...")
                continue
            uf = match_uf.group()

            # === Extrair filial do nome do usuário ===
            match_filial = re.search(r"\d+", chamado["NomeUsuario"])
            if not match_filial:
                log(f"❌ Não foi possível extrair número da filial no chamado {cod_chamado}")
                continue
            num_filial = int(match_filial.group())

            log(f"\n➡️  Chamado: {cod_chamado} | CR: {cod_cr} | UF: {uf} | Tipo: {tipo_cr}")

            # === Cadastro ===
            mensagem, tipo_cr = fun.cadastrar_prescritor(uf, cod_cr, tipo_cr, token_datahub)

            # === Interação ===
            fun.interagir_chamado(cod_chamado, token_desk, mensagem, tipo_cr)

    except Exception as e:
        log(f"❌ Erro ao listar/processar chamados: {e}")
