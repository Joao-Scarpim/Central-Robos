import requests
import re
from . import fun_cadastro_prescritor as fun


def run(ativo):

    log = fun.get_logger("cadastro_de_prescritor")

    url = "https://api.desk.ms/Login/autenticar"
    headers = {
        "Authorization": "30c55b0282a7962061dd41a654b6610d02635ddf",
        "JsonPath": "true"
    }
    payload = {
        "PublicKey": "1bb099a1915916de10c9be05ff4d2cafed607e7f"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            token = response_data["access_token"]
            log(f"Autenticação realizada com sucesso! Token: {token}")
        else:
            log(f"Erro na autenticação. Código: {response.status_code}")
            log(f"Mensagem: {response.text}")


    except Exception as e:
        log(f"Ocorreu um erro durante a autenticação: {e}")


# === LISTAR CHAMADOS ===
    url = "https://api.desk.ms/ChamadosSuporte/lista"
    headers = {
        "Authorization": f"{token}"
    }

    payload = {
        "Pesquisa": "CSN - CADASTRO DE PRESCRITOR",  #CSN - CADASTRO DE PRESCRITOR
        "Tatual": "",
        "Ativo": ativo,
        "StatusSLA": "",
        "Colunas": {
            "Chave": "on",
            "CodChamado": "on",
            "NomePrioridade": "on",
            "DataCriacao": "on",
            "HoraCriacao": "on",
            "DataFinalizacao": "on",
            "HoraFinalizacao": "on",
            "DataAlteracao": "on",
            "HoraAlteracao": "on",
            "NomeStatus": "on",
            "Assunto": "on",
            "Descricao": "on",
            "ChaveUsuario": "on",
            "NomeUsuario": "on",
            "SobrenomeUsuario": "on",
            "NomeCompletoSolicitante": "on",
            "SolicitanteEmail": "on",
            "NomeOperador": "on",
            "SobrenomeOperador": "on",
            "TotalAcoes": "on",
            "TotalAnexos": "on",
            "Sla": "on",
            "CodGrupo": "on",
            "NomeGrupo": "on",
            "CodSolicitacao": "on",
            "CodSubCategoria": "on",
            "CodTipoOcorrencia": "on",
            "CodCategoriaTipo": "on",
            "CodPrioridadeAtual": "on",
            "CodStatusAtual": "on",
            "_126143": "on",
            "_126157": "on",
            "_126152": "on"
        },
        "Ordem": [
            {
                "Coluna": "Chave",
                "Direcao": "true"
            }
        ]
    }

    try:
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            chamados = response_data["root"]

            for chamado in chamados:
                descricao = chamado["Descricao"]


            # Identificar número da filial
                regex_filial = r"\d+"
                filial = chamado["NomeUsuario"]
                match = re.search(regex_filial, filial)

                if match:
                    num_filial = int(match.group())
                    log(f"Filial identificada: {num_filial}")
                else:
                    log("Não foi possível identificar a filial.")
                    continue


                regex_uf = r'\b[A-Z]{2}\b'
                uf_prescritor = chamado["_126157"]
                tipo_cr = chamado["_126152"]
                match = re.search(regex_uf, uf_prescritor)

                if match:
                    uf = match.group()
                    log(f"UF PRESCRITOR: {uf}")
                else:
                    log("Não foi possível identificar a uf do prescritor.")
                    continue


                cod_chamado = chamado["CodChamado"]
                cod_cr = chamado["_126143"].strip()



                log(f"Chamado: {cod_chamado}")
                log(f"CÓDIGO CR: {cod_cr}")


                mensagem, tipo_cr = fun.cadastrar_prescritor(uf, cod_cr, tipo_cr)


                fun.interagir_chamado(cod_chamado, token, mensagem, tipo_cr)








            # Consultar notas e interagir no chamado


        else:
            print(f"Erro na requisição. Código: {response.status_code}")
            print(f"Mensagem: {response.text}")

    except Exception as e:
        print(f"Ocorreu um erro durante a requisição: {e}")

