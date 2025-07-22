# --- app_transformador.py (VERSÃO FINAL PARA NUVEM) ---

# Importações necessárias
import streamlit as st
import pandas as pd
import requests
import os
import io
import xlwt 
# A biblioteca docling é específica e você precisa garantir que ela esteja no seu requirements.txt
# Vamos assumir que ela existe, mas para o Render pode precisar de uma instalação especial.
# Por enquanto, focaremos na lógica.
# from docling.document_converter import DocumentConverter, PdfFormatOption
# from docling.datamodel.pipeline_options import PdfPipelineOptions
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import json

# --- CONFIGURAÇÃO ---
# O webhook do seu n8n que já está na nuvem
N8N_WEBHOOK_URL = "http://137.131.249.66:5678/webhook/822beb53-2a93-4496-bf35-2db926f2bef2" 
# O nome exato da sua planilha no Google Drive
NOME_PLANILHA_GOOGLE = "EstoqueStreamlitApp"

# --- FUNÇÕES DE GESTÃO DE DADOS (AGORA PARA GOOGLE SHEETS) ---

# Função para conectar ao Google Sheets de forma segura
@st.cache_resource
def conectar_google_sheets():
    """Conecta ao Google Sheets usando as credenciais armazenadas nos segredos do Streamlit."""
    try:
        creds_json = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_json)
        sh = sa.open(NOME_PLANILHA_GOOGLE)
        st.success(f"Conectado com sucesso à planilha '{NOME_PLANILHA_GOOGLE}'!")
        return sh
    except Exception as e:
        st.error(f"Erro ao conectar com o Google Sheets: {e}")
        st.info("Verifique se as credenciais 'gcp_service_account' estão configuradas nos segredos do seu app (Secrets).")
        return None

# Função para carregar os dados de uma aba específica
def carregar_aba_como_df(planilha_google, nome_da_aba, dtypes=None):
    """Carrega uma aba específica da Planilha Google como um DataFrame Pandas."""
    if planilha_google is None: return None
    try:
        ws = planilha_google.worksheet(nome_da_aba)
        # O header=1 significa que a primeira linha é o cabeçalho.
        df = get_as_dataframe(ws, dtype=dtypes or {}, header=1) 
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Aba '{nome_da_aba}' não encontrada na Planilha Google.")
        return None
    except Exception as e:
        st.error(f"Erro ao carregar a aba '{nome_da_aba}': {e}")
        return None

# Função para salvar um DataFrame de volta em uma aba
def salvar_df_na_aba(planilha_google, df, nome_da_aba):
    """Salva o DataFrame atualizado de volta na aba especificada no Google Sheets."""
    if planilha_google is None: return False
    try:
        ws = planilha_google.worksheet(nome_da_aba)
        ws.clear() # Limpa a planilha antiga antes de escrever a nova
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        st.success(f"Aba '{nome_da_aba}' no Google Sheets atualizada com sucesso!")
        return True
    except Exception as e:
        st.error(f"Ocorreu um erro inesperado ao salvar na aba '{nome_da_aba}': {e}")
        return False

# --- FUNÇÕES DE PROCESSAMENTO (IDÊNTICAS AO SEU CÓDIGO ORIGINAL) ---
# Você pode colar suas funções aqui, mas por clareza, vou usar placeholders
# para manter o foco nas mudanças.

def extrair_dados_do_pdf(arquivo_pdf_bytes):
    # SUA LÓGICA DE EXTRAÇÃO DE PDF VAI AQUI. ELA NÃO MUDA.
    # Por enquanto, retornaremos dados de exemplo para o código funcionar sem a biblioteca docling.
    st.info("Simulando extração de PDF...")
    dados_exemplo = [
        {'Cód. Produto / EAN*': '789001', 'Nome Produto*': 'Produto A', 'Unidade*': 'UN', 'Preço Custo': '10,50', 'Qtd. Estoque Atual': '10'},
        {'Cód. Produto / EAN*': '789002', 'Nome Produto*': 'Produto B', 'Unidade*': 'CX', 'Preço Custo': '25,00', 'Qtd. Estoque Atual': '5'}
    ]
    st.success("PDF simulado extraído com sucesso!")
    return dados_exemplo
    

def transformar_dados_via_n8n(dados_brutos):
    st.info(f"Preparando para enviar {len(dados_brutos)} itens para o n8n...")
    try:
        # A sua lógica de payload pode precisar de ajustes, mas o conceito é o mesmo
        payload = {"todos": dados_brutos, "primeiro": dados_brutos[0] if dados_brutos else {}}
        st.info(f"Enviando dados para o Webhook: {N8N_WEBHOOK_URL}")
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=300)
        st.info(f"n8n respondeu com o código de status: {response.status_code}")
        response.raise_for_status() 
        st.success("Dados processados pelo n8n com sucesso!")
        return response.json()
    except Exception as e:
        st.error(f"Erro ao comunicar com o n8n: {e}")
        return None

def dataframe_to_xls_bytes(df):
    st.info("Gerando arquivo .xls para download...")
    output = io.BytesIO()
    # Usando o engine 'xlsxwriter' que é mais moderno, mas 'xlwt' também funciona.
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='EstoqueAtualizado')
    return output.getvalue()


# --- LÓGICA DE MANIPULAÇÃO DE DADOS ---
# (Essa parte foi extraída da sua interface para ficar mais organizada)
def processar_e_atualizar_estoque(df_estoque_atual, df_novos_produtos, df_modelo):
    """Mescla os novos produtos com o estoque existente."""
    if 'QtdEstoqueAtual' not in df_estoque_atual.columns:
        df_estoque_atual['QtdEstoqueAtual'] = 0
    df_estoque_atual['QtdEstoqueAtual'] = pd.to_numeric(df_estoque_atual['QtdEstoqueAtual'], errors='coerce').fillna(0).astype(int)
    
    for _, produto_novo in df_novos_produtos.iterrows():
        ref_nova = str(produto_novo['REFERÊNCIA'])
        qtd_nova = int(produto_novo['QtdEstoqueAtual'])
        
        # Procura se a referência já existe no estoque
        if ref_nova in df_estoque_atual['REFERÊNCIA'].astype(str).values:
            # Se existe, soma a quantidade
            indice = df_estoque_atual.index[df_estoque_atual['REFERÊNCIA'].astype(str) == ref_nova].tolist()[0]
            df_estoque_atual.loc[indice, 'QtdEstoqueAtual'] += qtd_nova
        else:
            # Se não existe, adiciona a linha nova
            df_estoque_atual = pd.concat([df_estoque_atual, pd.DataFrame([produto_novo])], ignore_index=True)

    # Reordena e preenche colunas com base no modelo
    df_estoque_final = pd.DataFrame(df_estoque_atual, columns=df_modelo.columns).fillna('') 
    
    if 'QtdEstoqueAtual' in df_estoque_final.columns:
         df_estoque_final['QtdEstoqueAtual'] = pd.to_numeric(df_estoque_final['QtdEstoqueAtual'], errors='coerce').fillna(0).astype(int)

    return df_estoque_final


# --- INTERFACE E LÓGICA PRINCIPAL DO APLICATIVO ---
st.set_page_config(layout="wide")
st.title("Gerenciador de Estoque Inteligente 📦 (Versão Nuvem)")

# Conecta ao Google Sheets uma vez e armazena o objeto da planilha
planilha_g = conectar_google_sheets()

if planilha_g:
    # Carrega os dados para o estado da sessão para persistirem entre interações
    if 'df_estoque' not in st.session_state:
        st.session_state.df_estoque = carregar_aba_como_df(planilha_g, "EstoqueMestre", dtypes={'REFERÊNCIA': str})
    if 'df_modelo' not in st.session_state:
        st.session_state.df_modelo = carregar_aba_como_df(planilha_g, "PlanilhaModelo")

    # Verifica se o carregamento foi bem-sucedido
    if st.session_state.df_estoque is None or st.session_state.df_modelo is None: