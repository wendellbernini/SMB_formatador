# --- app_transformador.py (VERSÃO 6.0 - SOLUÇÃO FINAL, COMPLETA E ROBUSTA) ---

import streamlit as st
import pandas as pd
import requests
import os
import io
import gspread
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
import json
import xlwt


# --- CONFIGURAÇÃO ---
N8N_WEBHOOK_URL = "http://137.131.249.66:5678/webhook/822beb53-2a93-4496-bf35-2db926f2bef2" 
NOME_PLANILHA_GOOGLE = "EstoqueStreamlitApp"
CAMINHO_CREDENCIAL_LOCAL = os.path.join(".streamlit", "gcp_service_account.json")

# --- FUNÇÕES DE GESTÃO DE DADOS (REESCRITAS PARA MÁXIMA ROBUSTEZ) ---

# --- app_transformador.py ---

# ... (suas outras importações no topo do arquivo) ...

# --- FUNÇÕES DE GESTÃO DE DADOS (REESCRITAS PARA MÁXIMA ROBUSTEZ) ---

# SUBSTITUA A FUNÇÃO INTEIRA ABAIXO
@st.cache_resource
def conectar_google_sheets():
    """
    Conecta ao Google Sheets de forma inteligente, funcionando tanto localmente
    (com arquivo) quanto em produção no Render (com Variável de Ambiente).
    """
    try:
        # 1. Tenta usar o arquivo local (para desenvolvimento na sua máquina)
        if os.path.exists(CAMINHO_CREDENCIAL_LOCAL):
            st.info("Usando credencial de arquivo local.")
            sa = gspread.service_account(filename=CAMINHO_CREDENCIAL_LOCAL)
        # 2. Se o arquivo não existe, tenta usar a Variável de Ambiente (para o Render)
        else:
            st.info("Arquivo local não encontrado. Usando credencial da Variável de Ambiente.")
            # Pega a string JSON da variável de ambiente que você configurou no Render
            creds_str = os.environ.get("gcp_service_account_json")
            
            # Se a variável não estiver configurada, mostra um erro claro
            if not creds_str:
                st.error("ERRO: A Variável de Ambiente 'gcp_service_account_json' não foi encontrada ou está vazia no Render.")
                return None
            
            # Converte a string de volta para um dicionário que o gspread entende
            creds_json = json.loads(creds_str)
            sa = gspread.service_account_from_dict(creds_json)
        
        # O resto da função continua como antes
        sh = sa.open(NOME_PLANILHA_GOOGLE)
        st.success(f"Conectado com sucesso à planilha '{NOME_PLANILHA_GOOGLE}'!")
        return sh
        
    except Exception as e:
        # Se qualquer coisa der errado, mostra um erro detalhado
        st.error(f"FALHA CRÍTICA NA CONEXÃO: {e}")
        return None


def carregar_dados_completos(planilha_google, nome_da_aba):
    if planilha_google is None: return None, None
    try:
        ws = planilha_google.worksheet(nome_da_aba)
        todos_os_valores = ws.get_all_values()
        
        if not todos_os_valores or len(todos_os_valores) < 2:
            st.warning(f"A aba '{nome_da_aba}' está vazia ou não tem as 2 linhas de cabeçalho. Criando estrutura a partir do modelo.")
            ws_modelo = planilha_google.worksheet("PlanilhaModelo")
            todos_os_valores = ws_modelo.get_all_values()
            if not todos_os_valores or len(todos_os_valores) < 2:
                 st.error("Aba 'PlanilhaModelo' também está vazia. Não é possível continuar.")
                 return None, None
        
        df_completo = pd.DataFrame(todos_os_valores)
        df_cabecalhos = df_completo.iloc[:2].reset_index(drop=True)
        df_dados = df_completo.iloc[2:].reset_index(drop=True)
        df_dados.columns = df_cabecalhos.iloc[0].values
        
        return df_cabecalhos, df_dados
    except Exception as e:
        st.error(f"Erro ao carregar a aba '{nome_da_aba}': {e}"); return None, None

# --- app_transformador.py -> SUBSTITUA ESTA FUNÇÃO ---

def salvar_dados_completos(planilha_google, df_cabecalhos, df_dados_editados, nome_da_aba):
    """
    Salva os dados de volta no Google Sheets de forma robusta, garantindo a
    ordem das colunas, tratando células vazias e prevenindo o desalinhamento
    que cria colunas extras.
    """
    if planilha_google is None:
        return False
    try:
        # 1. Pega a lista exata e ordenada de nomes de colunas do cabeçalho original.
        #    Esta é a "verdade absoluta" da estrutura da nossa planilha.
        colunas_originais_ordenadas = df_cabecalhos.iloc[0].tolist()

        # 2. Reordena o DataFrame editado para que suas colunas correspondam
        #    exatamente à ordem das colunas originais. Isso lida com colunas que
        #    possam ter sido reordenadas ou excluídas no editor.
        df_dados_alinhado = df_dados_editados.reindex(columns=colunas_originais_ordenadas)

        # 3. Substitui todos os valores nulos/NaN por uma string vazia ('').
        #    Isso evita que o texto "NaN" seja escrito na planilha.
        df_dados_alinhado.fillna('', inplace=True)

        # --- A CORREÇÃO CRÍTICA ESTÁ AQUI ---
        # Antes de juntar (concatenar) os DataFrames de cabeçalho e de dados, eles
        # precisam ter os mesmos nomes de coluna para que o Pandas os empilhe
        # verticalmente. O df_cabecalhos tem colunas numéricas (0, 1, 2...).
        # Forçamos o df_dados_alinhado a ter as mesmas colunas numéricas.
        df_dados_alinhado.columns = df_cabecalhos.columns

        # 4. Agora, com os nomes das colunas correspondendo, a concatenação funciona
        #    como esperado, empilhando as linhas umas sobre as outras corretamente.
        df_final = pd.concat([df_cabecalhos, df_dados_alinhado], ignore_index=True)

        # 5. Acessa a aba correta na planilha.
        ws = planilha_google.worksheet(nome_da_aba)

        # 6. Converte o DataFrame final para o formato que a API do Google aceita
        #    (lista de listas), garantindo que todos os valores sejam strings.
        lista_de_valores = df_final.astype(str).values.tolist()

        # 7. Limpa a aba inteira e escreve os novos dados de uma só vez.
        #    Isso garante 100% de integridade estrutural.
        ws.clear()
        ws.update('A1', lista_de_valores, value_input_option='USER_ENTERED')

        return True

    except Exception as e:
        st.error(f"Ocorreu um erro inesperado ao salvar os dados: {e}")
        return False


def extrair_dados_do_pdf(arquivo_pdf_bytes):
    temp_file_path = "temp_nota_fiscal_para_processar.pdf"
    try:
        with open(temp_file_path, "wb") as f: f.write(arquivo_pdf_bytes)
        converter = DocumentConverter(format_options={"pdf": PdfFormatOption(pipeline_options=PdfPipelineOptions(do_table_structure=True))})
        resultado = converter.convert(temp_file_path)
        tabelas = [tabela.export_to_dataframe() for tabela in resultado.document.tables if 'COD. PROD.' in tabela.export_to_dataframe().columns]
        if tabelas:
            st.success("Tabelas de produtos extraídas com sucesso!")
            return pd.concat(tabelas, ignore_index=True).to_dict(orient='records')
        st.warning("Nenhuma tabela de produtos válida foi encontrada no PDF.")
        return None
    except Exception as e:
        st.error(f"Erro ao processar PDF: {e}"); return None
    finally:
        if os.path.exists(temp_file_path): os.remove(temp_file_path)

def transformar_dados_via_n8n(dados_brutos):
    st.info(f"Enviando {len(dados_brutos)} itens para o n8n...")
    try:
        response = requests.post(N8N_WEBHOOK_URL, json={"todos": dados_brutos}, timeout=300)
        response.raise_for_status() 
        st.success(f"n8n respondeu com sucesso (Status: {response.status_code})!")
        return response.json()
    except Exception as e:
        st.error(f"Erro ao comunicar com o n8n: {e}"); return None

# --- SUBSTITUA ESTA FUNÇÃO ---

# --- SUBSTITUA ESTA FUNÇÃO ---

# --- SUBSTITUA ESTA FUNÇÃO ---

def dataframe_to_xls_bytes(df_cabecalhos, df_dados):
    """
    Gera os bytes de um arquivo Excel (.xls) legado usando a biblioteca xlwt
    diretamente, garantindo compatibilidade com Pandas 2.0+.
    """
    # 1. Alinhar e concatenar os dataframes (lógica antiga que ainda é necessária)
    df_dados_copia = df_dados.copy()
    df_dados_copia.columns = df_cabecalhos.columns
    df_final = pd.concat([df_cabecalhos, df_dados_copia], ignore_index=True)

    # 2. Criar o arquivo Excel em memória usando xlwt
    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('EstoqueAtualizado')

    # 3. Escrever os cabeçalhos e os dados célula por célula
    for i, row in enumerate(df_final.values):
        for j, value in enumerate(row):
            # Converte todos os valores para string para evitar erros de tipo
            worksheet.write(i, j, str(value))

    # 4. Salvar o arquivo final no buffer de bytes
    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()

def processar_e_atualizar_estoque(df_estoque_atual, df_novos_produtos, df_cabecalho_modelo):
    col_ref = 'REFERÊNCIA'; col_qtd = 'QtdEstoqueAtual'
    if col_qtd not in df_estoque_atual.columns: df_estoque_atual[col_qtd] = 0
    df_estoque_atual[col_qtd] = pd.to_numeric(df_estoque_atual[col_qtd], errors='coerce').fillna(0).astype(int)
    
    for _, produto_novo in df_novos_produtos.iterrows():
        ref_nova = str(produto_novo.get(col_ref, ''))
        if ref_nova and ref_nova in df_estoque_atual[col_ref].astype(str).values:
            idx = df_estoque_atual.index[df_estoque_atual[col_ref].astype(str) == ref_nova][0]
            df_estoque_atual.loc[idx, col_qtd] += int(produto_novo.get(col_qtd, 0))
        else:
            df_estoque_atual = pd.concat([df_estoque_atual, pd.DataFrame([produto_novo])], ignore_index=True)
    
    df_estoque_final = df_estoque_atual.reindex(columns=df_cabecalho_modelo.iloc[0].tolist()).fillna('')
    if col_qtd in df_estoque_final.columns:
         df_estoque_final[col_qtd] = pd.to_numeric(df_estoque_final[col_qtd], errors='coerce').fillna(0).astype(int)
    return df_estoque_final

# --- INTERFACE E LÓGICA PRINCIPAL DO APLICATIVO ---
st.set_page_config(layout="wide")
st.title("Gerenciador de Estoque Inteligente 📦")

planilha_g = conectar_google_sheets()

if planilha_g:
    if 'cabecalho_estoque' not in st.session_state or 'dados_estoque' not in st.session_state:
        st.session_state.cabecalho_estoque, st.session_state.dados_estoque = carregar_dados_completos(planilha_g, "EstoqueMestre")
    if 'cabecalho_modelo' not in st.session_state:
        st.session_state.cabecalho_modelo, _ = carregar_dados_completos(planilha_g, "PlanilhaModelo")

    if st.session_state.dados_estoque is None or st.session_state.cabecalho_modelo is None:
        st.error("Falha crítica ao carregar dados. Verifique os nomes das abas e permissões."); st.stop()

    tab1, tab2, tab3 = st.tabs(["📤 Importar Notas", "✏️ Editar Estoque", "📥 Baixar Cópia"])

    with tab1:
        st.header("1. Importar Novas Notas Fiscais")
        uploaded_files = st.file_uploader("Escolha um ou mais arquivos PDF:", type="pdf", accept_multiple_files=True)
        if uploaded_files:
            informacoes_notas = {}
            for file in uploaded_files:
                with st.expander(f"⚙️ Configurar para: **{file.name}**"):
                    fabricante = st.text_input("FABRICANTE:", key=f"fab_{file.name}")
                    fornecedor = st.text_input("Forn_Prod:", key=f"forn_{file.name}")
                    informacoes_notas[file.name] = {"FABRICANTE": fabricante, "Forn_Prod": fornecedor, "file_object": file}
            if st.button("Processar e Salvar no Estoque", key="processar_notas"):
                with st.spinner('Processando todas as notas...'):
                    df_estoque_temp = st.session_state.dados_estoque.copy()
                    for file_name, info in informacoes_notas.items():
                        st.subheader(f"Processando: {file_name}")
                        dados_extraidos = extrair_dados_do_pdf(info["file_object"].getvalue())
                        if dados_extraidos:
                            novos_produtos_n8n = transformar_dados_via_n8n(dados_extraidos)
                            if novos_produtos_n8n:
                                df_novos = pd.DataFrame(novos_produtos_n8n)
                                mapa = {"Cód. Produto / EAN*": "REFERÊNCIA", "Nome Produto*": "Produto", "Unidade*": "unid", "Preço Custo": "Preço", "Qtd. Estoque Atual": "QtdEstoqueAtual"}
                                df_novos.rename(columns=mapa, inplace=True)
                                df_novos['QtdEstoqueAtual'] = pd.to_numeric(df_novos['QtdEstoqueAtual'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0).astype(int)
                                df_novos["FABRICANTE"] = info["FABRICANTE"]
                                df_novos["Forn_Prod"] = info["Forn_Prod"]
                                df_estoque_temp = processar_e_atualizar_estoque(df_estoque_temp, df_novos, st.session_state.cabecalho_modelo)
                    if salvar_dados_completos(planilha_g, st.session_state.cabecalho_estoque, df_estoque_temp, "EstoqueMestre"):
                        st.success("Estoque salvo com sucesso!")
                        st.session_state.dados_estoque = df_estoque_temp.copy()
                        st.rerun()

    # --- TRECHO DE CÓDIGO PARA SUBSTITUIR NA tab2 ---

   # --- SUBSTITUA O CONTEÚDO DA "with tab2:" ---

with tab2:
    st.header("2. Editar Estoque Mestre")
    st.info("Altere os dados diretamente na tabela abaixo. As alterações são salvas permanentemente ao clicar no botão.")

    # 1. Criar um dicionário de configuração de colunas inteligente.
    nomes_reais = st.session_state.cabecalho_estoque.iloc[0].tolist()
    nomes_amigaveis = st.session_state.cabecalho_estoque.iloc[1].tolist()
    
    # Lista de colunas que sabemos que devem ser numéricas.
    colunas_numericas = ['QtdEstoqueAtual', 'Preço', 'ML', 'PRECO_APRAZO', 'IPI', 'EstMínimo']

    configuracao_colunas = {}
    for nome_real, nome_amigavel in zip(nomes_reais, nomes_amigaveis):
        label = nome_amigavel if nome_amigavel else nome_real
        
        # --- A CORREÇÃO CRÍTICA ESTÁ AQUI ---
        # Verifica se a coluna atual deve ser tratada como número.
        if nome_real in colunas_numericas:
            configuracao_colunas[nome_real] = st.column_config.NumberColumn(
                label=label,
                format="%d" if nome_real == 'QtdEstoqueAtual' else "%.2f" # Formato para inteiro ou decimal
            )
        else:
            # Caso contrário, usa o editor de texto padrão.
            configuracao_colunas[nome_real] = st.column_config.TextColumn(
                label=label
            )

    # 2. Exibir o data_editor com a configuração de coluna correta.
    dados_editados = st.data_editor(
        st.session_state.dados_estoque,
        column_config=configuracao_colunas,
        num_rows="dynamic",
        key="data_editor_dados",
        use_container_width=True
    )

    if st.button("✅ Salvar Alterações no Estoque Mestre", type="primary", key="salvar_estoque"):
        with st.spinner("Salvando..."):
            if salvar_dados_completos(planilha_g, st.session_state.cabecalho_estoque, dados_editados, "EstoqueMestre"):
                st.success("Salvo com sucesso! Recarregando...")
                st.session_state.dados_estoque = dados_editados.copy()
                st.rerun()

   # --- SUBSTITUA O CONTEÚDO DA "with tab3:" ---

with tab3:
    st.header("3. Visualizar e Baixar Cópia Local")

    # Reutiliza a mesma lógica da "tab2" para criar cabeçalhos amigáveis.
    nomes_reais = st.session_state.cabecalho_estoque.iloc[0].tolist()
    nomes_amigaveis = st.session_state.cabecalho_estoque.iloc[1].tolist()
    
    colunas_numericas = ['QtdEstoqueAtual', 'Preço', 'ML', 'PRECO_APRAZO', 'IPI', 'EstMínimo']

    configuracao_colunas = {}
    for nome_real, nome_amigavel in zip(nomes_reais, nomes_amigaveis):
        label = nome_amigavel if nome_amigavel else nome_real
        
        if nome_real in colunas_numericas:
            configuracao_colunas[nome_real] = st.column_config.NumberColumn(
                label=label,
                format="%d" if nome_real == 'QtdEstoqueAtual' else "%.2f"
            )
        else:
            configuracao_colunas[nome_real] = st.column_config.TextColumn(
                label=label
            )

    # Exibe os dados do estoque (em modo de leitura) com a configuração correta.
    st.dataframe(
        st.session_state.dados_estoque,
        column_config=configuracao_colunas,
        hide_index=True,
        use_container_width=True
    )

    if not st.session_state.dados_estoque.empty:
        xls_bytes = dataframe_to_xls_bytes(st.session_state.cabecalho_estoque, st.session_state.dados_estoque)
        st.download_button(
            "Baixar Cópia Local (.xls)",
            data=xls_bytes,
            file_name="EstoqueMestre_CopiaLocal.xls",
            mime="application/vnd.ms-excel"
        )