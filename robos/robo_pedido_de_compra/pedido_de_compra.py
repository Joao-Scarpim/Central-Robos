import requests
import re
from . import fun_pedido_de_compra as fun


def run(ativo):
    # === CONFIGURAÇÃO DO LOG ===

    # Diretório onde os logs serão salvos
    log = fun.get_logger("pedidos_de_compra")

    # === AUTENTICAÇÃO NA API ===
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
            return

    except Exception as e:
        log(f"Ocorreu um erro durante a autenticação: {e}")
        return

    # === LISTAR CHAMADOS ===
    url = "https://api.desk.ms/ChamadosSuporte/lista"
    headers = {
        "Authorization": f"{token}"
    }

    payload = {
        "Pesquisa": "CSN - PEDIDO COMPLEMENTAR",
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
            "_6313": "on"
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
                # Extrair chaves da descrição
                descricao = chamado["Descricao"]
                padrao = r"\b\d{44}\b"
                chaves_nf = re.findall(padrao, descricao)
                log(f"Chaves encontradas: {chaves_nf}")

                # Identificar número da filial
                regex_filial = r"\d+"
                filial = chamado["NomeUsuario"]
                cod_chamado = chamado["CodChamado"]
                log(f"Chamado: {cod_chamado}")
                match = re.search(regex_filial, filial)

                if match:
                    num_filial = int(match.group())
                    log(f"Filial identificada: {num_filial}")
                else:
                    log("Não foi possível identificar a filial.")
                    continue

                # Consultar notas e interagir no chamado
                notas_com_pedido, notas_sem_pedido, notas_nao_encontradas, notas_nao_loja, notas_gerado_pedido = fun.consultar_pedidos_notas(
                    num_filial, chaves_nf, num_filial
                )

                fun.interagir_chamado(
                    cod_chamado,
                    token,
                    notas_com_pedido,
                    notas_sem_pedido,
                    notas_nao_encontradas,
                    notas_nao_loja,
                    notas_gerado_pedido
                )

        else:
            log(f"Erro na requisição. Código: {response.status_code}")
            log(f"Mensagem: {response.text}")

    except Exception as e:
        log(f"Ocorreu um erro durante a requisição: {e}")
