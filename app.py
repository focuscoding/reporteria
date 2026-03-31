import streamlit as st
from datetime import date, timedelta
import modulo_farmago
import modulo_general



st.set_page_config(page_title="Sistema de Reportes Odoo", layout="wide")

# --- SELECTOR DE FECHAS GLOBAL ---
st.sidebar.title("📅 Filtros Globales")
hoy = date.today()

# lunes de esta semana
lunes_semana_actual = hoy - timedelta(days=hoy.weekday())

lunes_anterior = lunes_semana_actual - timedelta(days=7)
domingo_anterior = lunes_semana_actual - timedelta(days=1)

col1, col2 = st.sidebar.columns(2)
with col1: 
    f_inicio = st.date_input(
    "Fecha inicio",
    value=lunes_anterior,
    format="DD/MM/YYYY"
)
with col2:
    f_fin = st.date_input(
    "Fecha fin",
    value=domingo_anterior,
    format="DD/MM/YYYY"
)

#definiendo parametros para ver si cambia
parametros_actuales = (f_inicio, f_fin)

if "parametros_previos" not in st.session_state:
    st.session_state.parametros_previos = parametros_actuales

if parametros_actuales != st.session_state.parametros_previos:

    claves_reset = [
        "df_farmago",
        "nombre_archivo",
        "df_resultado",
        "archivos_binarios",
        "tipo_reporte_activo",
        "config_costos"
    ]

    for k in claves_reset:
        if k in st.session_state:
            del st.session_state[k]

    st.session_state.parametros_previos = parametros_actuales

# --- NAVEGACIÓN ---
opcion = st.sidebar.radio("Seleccione Reporte", ["Facturación Farmago", "Reportes Sell-Out"])

st.divider()

if opcion == "Facturación Farmago":
    # Pasamos las fechas como argumentos a la función del módulo
    modulo_farmago.render_reporte(f_inicio, f_fin)
else:
    modulo_general.render_reporte(f_inicio, f_fin)
