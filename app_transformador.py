# --- app_transformador.py (VERSÃO CORRIGIDA E FINAL) ---

# Importações necessárias
import streamlit as st
import pandas as pd
import requests
import os
import io
import xlwt 
# A biblioteca docling precisa estar no seu requirements.txt
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import json

# --- CONFIGURAÇÃO ---
N8N_WEBHOOK_URL = "http://137.131.249.66:5678/webhook/822beb53-2a93-4496-bf35-2db926f2bef2" 
NOME_PLANILHA_GOOGLE = "EstoqueStreamlitApp"

# --- FUNÇÕES DE GESTÃO DE DADOS (PARA GOOGLE SHEETS) ---

@st.cache_resource # Cacheia a conexão para não reconectar a cada interação
def conectar_google_sheets():
    try:
        creds_json = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_json)
        sh = sa.open(NOME_PLANILHA_GOOGLE)
        st.success(f"Conectado com sucesso à planilha '{NOME_PLANILHA_GOOGLE}'!")
        return sh
    except Exception as e:
        st.error(f"Erro ao conectar com o Google Sheets: {e}")
        st.info("Verifique se as credenciais 'gcp_service_account' estão configuradas nos segredos do seu app.")
        return None

def carregar_aba_como_df(planilha_google, nome_da_aba, dtypes=None):
    if planilha_google is None: return None
    try:
        ws = planilha_google.worksheet(nome_da_aba)
        df = get_as_dataframe(ws, dtype=dtypes or {}, header=1) 
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Aba '{nome_da_aba}' não encontrada na Planilha Google.")
        return None
    except Exception as e:
        st.error(f"Erro ao carregar a aba '{nome_da_aba}': {e}")
        return None

def salvar_df_na_aba(planilha_google, df, nome_da_aba):
    if planilha_google is None: return False
    try:
        ws = planilha_google.worksheet(nome_da_aba)
        ws.clear()
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        return True # Retorna True em caso de sucesso
    except Exception as e:
        st.error(f"Ocorreu um erro inesperado ao salvar na aba '{nome_da_aba}': {e}")
        return False

# --- FUNÇÕES DE PROCESSAMENTO (SUA LÓGICA ORIGINAL) ---

def extrair_dados_do_pdf(arquivo_pdf_bytes):
    temp_file_path = "temp_nota_fiscal_para_processar.pdf"
    try:
        st.write("Criando arquivo temporário para processamento...")
        with open(temp_file_path, "wb") as f:
            f.write(arquivo_pdf_bytes)
        pipeline_options = PdfPipelineOptions(do_table_structure=True)
        format_options = {"pdf": PdfFormatOption(pipeline_options=pipeline_options)}
        converter = DocumentConverter(format_options=format_options)
        st.write("Extraindo tabelas do PDF...")
        resultado = converter.convert(temp_file_path)
        todas_as_tabelas = []
        if resultado.document.tables:
            for tabela in resultado.document.tables:
                df = tabela.export_to_dataframe()
                if 'COD. PROD.' in df.columns:
                    todas_as_tabelas.append(df)
            if todas_as_tabelas:
                st.success("Tabelas de produtos extraídas com sucesso!")
                tabela_completa = pd.concat(todas_as_tabelas, ignore_index=True)
                return tabela_completa.to_dict(orient='records')
        st.error("Nenhuma tabela de produtos válida foi encontrada no PDF.")
        return None
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar o PDF com a biblioteca Docling: {e}")
        return None
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            st.write("Arquivo temporário removido.")


def transformar_dados_via_n8n(dados_brutos):
    st.info(f"Preparando para enviar {len(dados_brutos)} itens para o n8n...")
    try:
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
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='EstoqueAtualizado')
    return output.getvalue()

def processar_e_atualizar_estoque(df_estoque_atual, df_novos_produtos, df_modelo):
    """Mescla os novos produtos com o estoque existente."""
    if 'QtdEstoqueAtual' not in df_estoque_atual.columns:
        df_estoque_atual['QtdEstoqueAtual'] = 0
    df_estoque_atual['QtdEstoqueAtual'] = pd.to_numeric(df_estoque_atual['QtdEstoqueAtual'], errors='coerce').fillna(0).astype(int)
    
    for _, produto_novo in df_novos_produtos.iterrows():
        ref_nova = str(produto_novo['REFERÊNCIA'])
        qtd_nova = int(produto_novo['QtdEstoqueAtual'])
        
        if ref_nova in df_estoque_atual['REFERÊNCIA'].astype(str).values:
            indice = df_estoque_atual.index[df_estoque_atual['REFERÊNCIA'].astype(str) == ref_nova].tolist()[0]
            df_estoque_atual.loc[indice, 'QtdEstoqueAtual'] += qtd_nova
        else:
            df_estoque_atual = pd.concat([df_estoque_atual, pd.DataFrame([produto_novo])], ignore_index=True)

    df_estoque_final = pd.DataFrame(df_estoque_atual, columns=df_modelo.columns).fillna('') 
    
    if 'QtdEstoqueAtual' in df_estoque_final.columns:
         df_estoque_final['QtdEstoqueAtual'] = pd.to_numeric(df_estoque_final['QtdEstoqueAtual'], errors='coerce').fillna(0).astype(int)

    return df_estoque_final


# --- INTERFACE E LÓGICA PRINCIPAL DO APLICATIVO ---
st.set_page_config(layout="wide")
st.title("Gerenciador de Estoque Inteligente 📦 (Versão Nuvem)")

planilha_g = conectar_google_sheets()

if planilha_g:
    if 'df_estoque' not in st.session_state:
        st.session_state.df_estoque = carregar_aba_como_df(planilha_g, "EstoqueMestre", dtypes={'REFERÊNCIA': str})
    if 'df_modelo' not in st.session_state:
        st.session_state.df_modelo = carregar_aba_como_df(planilha_g, "PlanilhaModelo")

    # ----- INÍCIO DA CORREÇÃO DO ERRO DE INDENTAÇÃO -----
    if st.session_state.df_estoque is None or st.session_state.df_modelo is None:
        st.error("Falha ao carregar dados essenciais da planilha. O aplicativo não pode continuar. Verifique os nomes das abas.")
        st.stop()
    # ----- FIM DA CORREÇÃO DO ERRO DE INDENTAÇÃO -----

    tab1, tab2, tab3 = st.tabs(["📤 Importar Notas", "✏️ Editar Estoque", "📥 Baixar Cópia"])

    with tab1:
        st.header("1. Importar Novas Notas Fiscais")
        uploaded_files = st.file_uploader("Escolha um ou mais arquivos PDF:", type="pdf", accept_multiple_files=True)

        if uploaded_files:
            informacoes_notas = {}
            for file in uploaded_files:
                with st.expander(f"⚙️ Configurar informações para: **{file.name}**"):
                    fabricante = st.text_input("FABRICANTE:", key=f"fab_{file.name}")
                    fornecedor = st.text_input("Forn_Prod:", key=f"forn_{file.name}")
                    informacoes_notas[file.name] = {"FABRICANTE": fabricante, "Forn_Prod": fornecedor, "file_object": file}

            if st.button("Processar Notas e Pré-visualizar", key="processar_notas"):
                with st.spinner('Aguarde... Processando todas as notas!'):
                    df_estoque_temporario = st.session_state.df_estoque.copy()
                    
                    for file_name, info in informacoes_notas.items():
                        st.subheader(f"Processando: {file_name}")
                        dados_extraidos = extrair_dados_do_pdf(info["file_object"].getvalue())
                        
                        if dados_extraidos:
                            novos_produtos_n8n = transformar_dados_via_n8n(dados_extraidos)
                            if novos_produtos_n8n:
                                df_novos_produtos = pd.DataFrame(novos_produtos_n8n)
                                mapa_nomes_finais = {
                                    "Cód. Produto / EAN*": "REFERÊNCIA", "Nome Produto*": "Produto", "Unidade*": "unid",
                                    "Preço Custo": "Preço", "Qtd. Estoque Atual": "QtdEstoqueAtual"
                                }
                                df_novos_produtos.rename(columns=mapa_nomes_finais, inplace=True)
                                df_novos_produtos['QtdEstoqueAtual'] = pd.to_numeric(df_novos_produtos['QtdEstoqueAtual'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0).astype(int)
                                df_novos_produtos["FABRICANTE"] = info["FABRICANTE"]
                                df_novos_produtos["Forn_Prod"] = info["Forn_Prod"]
                                df_estoque_temporario = processar_e_atualizar_estoque(df_estoque_temporario, df_novos_produtos, st.session_state.df_modelo)
                    
                    st.session_state.df_estoque = df_estoque_temporario
                    st.success("Notas processadas! Vá para a aba 'Editar Estoque' para revisar e salvar.")
                    st.rerun() # Atualiza a tela para refletir os novos dados na outra aba
    
    with tab2:
        st.header("2. Editar Estoque Mestre")
        st.info("Aqui você pode corrigir, adicionar ou remover linhas. As alterações só serão permanentes após clicar em 'Salvar'.")
        
        df_editado = st.data_editor(
            st.session_state.df_estoque, num_rows="dynamic", key="data_editor_estoque", use_container_width=True
        )

        if st.button("✅ Salvar Alterações no Estoque Mestre", type="primary", key="salvar_estoque"):
            with st.spinner("Salvando no Google Sheets..."):
                st.session_state.df_estoque = df_editado
                if salvar_df_na_aba(planilha_g, st.session_state.df_estoque, "EstoqueMestre"):
                    st.success("Estoque salvo com sucesso!")
                else:
                    st.error("Falha ao salvar o estoque.")

    with tab3:
        st.header("3. Visualizar e Baixar Cópia Local")
        st.dataframe(st.session_state.df_estoque, use_container_width=True)
        
        if not st.session_state.df_estoque.empty:
            xls_bytes = dataframe_to_xls_bytes(st.session_state.df_estoque)
            st.download_button(
                label="Baixar Cópia Local do Estoque (.xlsx)",
                data=xls_bytes,
                file_name="EstoqueMestre_CopiaLocal.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )