#modulo_consolidado
# ─────────────────────────────────────────────────────────────────
# Combina la lógica de modulo_general_libre y modulo_general_excluyente
# en un único flujo, con configuración de comportamiento por laboratorio:
#
#   - Por defecto (LIBRE): si una venta tiene descuento vigente en SellOut
#     Y en CT (Hoja1/Detalle), se generan lineas correspondientes recalculando el precio base desde SellOut
#   - Si el laboratorio se marca "Excluir": cuando una venta tiene ambos
#     descuentos, se conserva solo el mayor de los dos (esa línea se reporta
#     una sola vez, con el descuento ganador), con el precio base desde Sellout.
#   - Si el laboratorio se marca "A Costo": el precio unitario del Excel se
#     reemplaza por el costo de laboratorio (product.supplierinfo), sin
#     importar el modo de descuento
#
# No se generan correos ni se envía resumen a Google Sheets: solo se genera
# un DataFrame consolidado y UN ÚNICO Excel descargable con todos los
# laboratorios, con el mismo formato/columnas que los módulos originales.
# ─────────────────────────────────────────────────────────────────

import streamlit as st
import pandas as pd
import io
import time
from datetime import date
from odoo_utils import OdooClient
import numpy as np
import unicodedata
import requests
import urllib.parse

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
GID_HOJA1   = 0
GID_EXCLUIR = 1591504897
GID_DETALLE = 150387248

CADENAS_FARMAGO_FARMATENCION = {'farmago', 'farmatencion'}  # sin tildes para comparación

URL_SO_DEFAULT = "https://docs.google.com/spreadsheets/d/1c4Eil9IoOhUTNr3_jrZn5HI5GNZq9NTkgPH0CbjwYMA/export?format=csv"
URL_CT_DEFAULT = "https://docs.google.com/spreadsheets/d/1R6xw2K5sHyRIMDAlr3fn0xNJewy8mRsiYf58TH1A-EA/export?format=csv"

REINTENTOS_ODOO      = 3
ESPERA_REINTENTO_SEG = 4  

#Para separacion de reportes y emails

TIPO_LABELS = {
    'sellout':      'SELL-OUT',
    'ct_lineal':    'Descuentos CT Lineal',
    'farmago':      'Farmago',
    'farmatencion': 'Farmatención',
}

def _slug_lab(lab):
    """Nombre de laboratorio apto para nombre de archivo."""
    return (
        lab.replace(" ", "_").replace("/", "").replace("\\", "").replace(":", "")
        .replace("á", "a").replace("é", "e").replace("í", "i")
        .replace("ó", "o").replace("ú", "u").replace("Á", "A")
        .replace("É", "E").replace("Í", "I").replace("Ó", "O")
        .replace("Ú", "U").replace("ü", "u").replace("Ü", "U")
        .replace("ñ", "n").replace("Ñ", "N")
    )


# ─────────────────────────────────────────────
# UTILIDADES (idénticas a los módulos originales)
# ─────────────────────────────────────────────

def estandarizar_barcodes(serie):
    return serie.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

def quitar_tildes(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s))
                   if unicodedata.category(c) != 'Mn')

def limpiar_odoo(val):
    return val[1] if isinstance(val, (list, tuple)) else val

def url_con_gid(url_base, gid):
    """Construye URL CSV para una pestaña específica del Sheets."""
    base = url_base.split('?')[0]
    return f"{base}?format=csv&gid={gid}"

# ─────────────────────────────────────────────
# PRESET DE CONFIGURACIÓN POR LABORATORIO
# ─────────────────────────────────────────────
# Se aplica SOLO la primera vez que un laboratorio es detectado en la sesión.
# Si el usuario cambia manualmente una casilla, esa elección se respeta y no
# se sobreescribe en detecciones posteriores dentro de la misma sesión.
PRESET_CONFIG_LAB = {
    'calox international, c.a.':    {'excluir': True,  'costo': False},
    'laboratorios farma, s.a.':     {'excluir': True,  'costo': False},
    'laboratorios siegfried s.a.':  {'excluir': True,  'costo': False},
    'especialidades dollder, c.a.': {'excluir': False, 'costo': True},
    'laboratorios vargas, s.a': {'excluir': False, 'costo': True},
}

def _normalizar_nombre_lab(nombre):
    """Normaliza nombre de laboratorio para matchear contra el preset,
    sin importar mayúsculas, tildes o espacios extra."""
    return ' '.join(quitar_tildes(str(nombre)).strip().lower().split())

# Preset ya normalizado, calculado una sola vez
PRESET_CONFIG_LAB_NORM = {
    _normalizar_nombre_lab(k): v for k, v in PRESET_CONFIG_LAB.items()
}

# ─────────────────────────────────────────────
# CONEXIÓN A ODOO — cacheada por sesión + reintentos ante caídas transitorias
# (ej. 503 Service Unavailable durante mantenimiento o alta carga del server)
# ─────────────────────────────────────────────

def get_odoo_client():
    """Reutiliza una única conexión por sesión de Streamlit en vez de abrir
    una nueva cada vez que se necesita consultar Odoo."""
    if '_odoo_client' not in st.session_state or st.session_state._odoo_client is None:
        config = st.secrets["odoo_bd1"]
        st.session_state._odoo_client = OdooClient(
            config["url"], config["db"], config["username"], config["password"]
        )
    return st.session_state._odoo_client


def odoo_search_read(model, domain, fields, intentos=REINTENTOS_ODOO, espera_seg=ESPERA_REINTENTO_SEG):
    """
    Wrapper de client.search_read con reintentos ante errores transitorios
    del servidor (503 Service Unavailable, timeouts de red, etc.).
    Si tras todos los intentos sigue fallando, relanza la última excepción.
    """
    ultimo_error = None
    for intento in range(1, intentos + 1):
        try:
            client = get_odoo_client()
            return client.search_read(model, domain, fields)
        except Exception as e:
            ultimo_error = e
            es_ultimo_intento = intento == intentos
            if not es_ultimo_intento:
                st.caption(
                    f"⏳ Odoo no respondió (intento {intento}/{intentos}: {e}). "
                    f"Reintentando en {espera_seg}s..."
                )
                # Si falló por una conexión cacheada rota, forzamos reconexión en el próximo intento
                st.session_state._odoo_client = None
                time.sleep(espera_seg)
    raise ultimo_error


# ─────────────────────────────────────────────
# LECTURA DE SHEETS (idéntica a los módulos originales, con pequeños
# agregados para poder mostrar nombre "bonito" de laboratorio en la UI)
# ─────────────────────────────────────────────

def obtener_ofertas_sheets(url):
    """
    Sheets SellOut: A=barcode, D=aplica_cliente, E=descuento, F=nc_check, H=inicio, I=fin.
    Solo filas NC. Devuelve descuento_so normalizado + aplica_cliente normalizado
    (sin tildes, minúsculas) para poder filtrar por cliente/cadena.
    """
    try:
        df = pd.read_csv(url)
        df = df.rename(columns={
            df.columns[0]: 'barcode_key',
            df.columns[3]: 'aplica_cliente',
            df.columns[4]: 'descuento_valor',
            df.columns[5]: 'nc_check',
            df.columns[7]: 'oferta_inicio',
            df.columns[8]: 'oferta_fin',
        })
        df['nc_check']    = df['nc_check'].astype(str).str.upper().str.strip()
        df                = df[df['nc_check'] == 'NC'].copy()
        df['barcode_key'] = estandarizar_barcodes(df['barcode_key'])
        df['aplica_cliente'] = (
            df['aplica_cliente'].astype(str).str.strip().str.lower().apply(quitar_tildes)
        )
        df['oferta_inicio'] = pd.to_datetime(df['oferta_inicio'], errors='coerce').dt.date
        df['oferta_fin']    = pd.to_datetime(df['oferta_fin'],    errors='coerce').dt.date
        df['descuento_so']  = (
            df['descuento_valor'].astype(str)
            .str.replace('%', '', regex=False).str.strip()
            .astype(float) / 100
        )
        return df
    except Exception as e:
        st.error(f"Error en Google Sheets SellOut: {e}")
        return pd.DataFrame()


def filtrar_por_aplica_cliente(df_match):
    """
    Filtra líneas de SellOut ya matcheadas según la columna 'aplica_cliente' del
    Sheets (misma lógica que modulo_general_libre):
      - contiene 'independiente' → excluye clientes cuya cadena sea 'farmago'
      - contiene 'farmago'       → incluye solo clientes cuya cadena sea 'farmago'
      - cualquier otro valor ('todos', vacío, etc.) → sin filtro, aplica a toda la venta

    Usa coincidencia parcial (contains) en vez de igualdad exacta, para tolerar
    variaciones de texto en el Sheets. Deduplica por línea de factura ('id')
    dando prioridad a la regla más específica (independientes/farmago) sobre
    'todos', evitando ventas duplicadas si dos filas del Sheets matchean el
    mismo barcode.

    Requiere que df_match tenga las columnas 'cadena_val', 'aplica_cliente' e 'id'.
    """
    if 'aplica_cliente' not in df_match.columns or df_match.empty:
        return df_match

    df_match = df_match.copy()

    cadena_norm = df_match['cadena_val'].apply(
        lambda x: quitar_tildes((x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower())
        if x else ''
    )
    aplica_norm = df_match['aplica_cliente'].astype(str)  # ya viene normalizado (sin tildes, lower)

    es_independientes = aplica_norm.str.contains('independiente', na=False)
    es_farmago         = aplica_norm.str.contains('farmago', na=False)
    es_todos            = ~(es_independientes | es_farmago)

    # ── Diagnóstico: valores que no matchean ninguna categoría conocida ──
    valores_no_reconocidos = sorted(set(aplica_norm[es_todos]) - {'', 'nan', 'todos'})
    if valores_no_reconocidos:
        st.warning(
            f"⚠️ 'aplica_cliente' con valores no reconocidos (tratados como 'Todos'): "
            f"{', '.join(valores_no_reconocidos)}"
        )

    mask = (
        (es_independientes & (cadena_norm != 'farmago')) |
        (es_farmago & (cadena_norm == 'farmago')) |
        es_todos
    )

    n_excl = (~mask).sum()
    if n_excl > 0:
        st.caption(f"ℹ️ SellOut: {n_excl} línea(s) excluidas por regla 'aplica_cliente'.")

    df_match = df_match[mask].copy()

    # ── Red de seguridad: dedupe por línea de factura ─────────────────
    # Si tras el filtro aún quedan 2+ filas para la misma línea (p.ej. porque
    # el texto no matcheó ninguna categoría y ambas cayeron en 'todos'),
    # priorizamos la fila más específica: farmago/independientes > todos.
    if 'id' in df_match.columns:
        df_match['_prioridad'] = np.select(
            [es_farmago[mask], es_independientes[mask]],
            [2, 2],
            default=1
        )
        n_antes = len(df_match)
        df_match = (
            df_match.sort_values('_prioridad', ascending=False)
            .drop_duplicates(subset=['id'], keep='first')
            .drop(columns=['_prioridad'])
        )
        n_dedup = n_antes - len(df_match)
        if n_dedup > 0:
            st.warning(
                f"⚠️ {n_dedup} línea(s) duplicada(s) por múltiples matches en 'aplica_cliente' "
                f"fueron deduplicadas automáticamente. Revisa el Sheets: probablemente hay texto "
                f"inconsistente en esa columna."
            )

    return df_match


def obtener_ct_hoja1(url_base):
    """
    Pestaña Hoja1 (gid=0): A=partner_name, B=cadena, C=laboratorio, D=descuento(%)
    G=vigencia_inicio, H=vigencia_fin. Descarta filas con descuento no numérico.
    """
    try:
        df = pd.read_csv(url_con_gid(url_base, GID_HOJA1))
        df = df.rename(columns={
            df.columns[0]: 'partner_name',
            df.columns[1]: 'cadena',
            df.columns[2]: 'laboratorio',
            df.columns[3]: 'descuento_valor_raw',
            df.columns[6]: 'vigencia_inicio',
            df.columns[7]: 'vigencia_fin',
        })

        desc_limpio   = df['descuento_valor_raw'].astype(str).str.replace('%', '', regex=False).str.strip()
        filas_validas = pd.to_numeric(desc_limpio, errors='coerce').notna()
        descartadas   = (~filas_validas).sum()
        if descartadas > 0:
            st.caption(f"⚠️ Hoja1 CT: {descartadas} fila(s) con descuento no numérico ignoradas.")
        df = df[filas_validas].copy()

        df['descuento_ct']     = desc_limpio[filas_validas].astype(float) / 100
        df['partner_name_key'] = df['partner_name'].astype(str).str.strip().str.lower()
        df['laboratorio_key']  = df['laboratorio'].astype(str).str.strip().str.lower()
        df['cadena_key']       = df['cadena'].astype(str).str.strip().str.lower().apply(quitar_tildes)
        df['vigencia_inicio']  = pd.to_datetime(df['vigencia_inicio'], errors='coerce').dt.date
        df['vigencia_fin']     = pd.to_datetime(df['vigencia_fin'],    errors='coerce').dt.date

        return df[['partner_name', 'partner_name_key', 'cadena', 'cadena_key',
                    'laboratorio', 'laboratorio_key', 'descuento_ct',
                    'vigencia_inicio', 'vigencia_fin']]
    except Exception as e:
        st.error(f"Error leyendo Hoja1 CT: {e}")
        return pd.DataFrame()


def obtener_ct_detalle(url_base):
    """
    Pestaña Detalle (gid=150387248): A=barcode, B=laboratorio, C=cadena_o_cliente, D=descuento(%)
    F=det_inicio, G=det_fin. Descarta filas con descuento no numérico.
    """
    try:
        df = pd.read_csv(url_con_gid(url_base, GID_DETALLE))
        df = df.rename(columns={
            df.columns[0]: 'barcode_det',
            df.columns[1]: 'laboratorio_det',
            df.columns[2]: 'cadena_cliente_det',
            df.columns[3]: 'descuento_det_raw',
        })

        desc_limpio   = df['descuento_det_raw'].astype(str).str.replace('%', '', regex=False).str.strip()
        filas_validas = pd.to_numeric(desc_limpio, errors='coerce').notna()
        descartadas   = (~filas_validas).sum()
        if descartadas > 0:
            st.caption(f"⚠️ Detalle CT: {descartadas} fila(s) con descuento no numérico ignoradas.")
        df = df[filas_validas].copy()

        df['descuento_det']          = desc_limpio[filas_validas].astype(float) / 100
        df['barcode_det']            = estandarizar_barcodes(df['barcode_det'])
        df['laboratorio_det']        = df['laboratorio_det'].astype(str).str.strip()
        df['laboratorio_det_key']    = df['laboratorio_det'].str.lower()
        df['cadena_cliente_det_key'] = df['cadena_cliente_det'].astype(str).str.strip().str.lower().apply(quitar_tildes)

        df['det_inicio'] = pd.to_datetime(df.iloc[:, 5], errors='coerce').dt.date
        df['det_fin']    = pd.to_datetime(df.iloc[:, 6], errors='coerce').dt.date

        return df[['barcode_det', 'laboratorio_det', 'laboratorio_det_key',
                    'cadena_cliente_det_key', 'descuento_det', 'det_inicio', 'det_fin']]
    except Exception as e:
        st.warning(f"⚠️ No se pudo leer pestaña Detalle CT: {e}")
        return pd.DataFrame()


def obtener_excluidos_ct(url_base):
    """
    Pestaña Excluir (gid=1591504897): A=barcode, B=laboratorio
    Productos SIN descuento CT para ese laboratorio (lista de exclusión de reglas,
    NO tiene relación con la casilla "Excluir" por laboratorio de este módulo).
    """
    try:
        df = pd.read_csv(url_con_gid(url_base, GID_EXCLUIR))
        df = df.rename(columns={
            df.columns[0]: 'barcode_excluido',
            df.columns[1]: 'laboratorio_excluir',
        })
        df['barcode_excluido']        = estandarizar_barcodes(df['barcode_excluido'])
        df['laboratorio_excluir_key'] = df['laboratorio_excluir'].astype(str).str.strip().str.lower()
        return df[['barcode_excluido', 'laboratorio_excluir_key']]
    except Exception as e:
        st.warning(f"⚠️ No se pudo leer pestaña Excluir CT: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
# DETECCIÓN DE LABORATORIOS
# ─────────────────────────────────────────────

def _vigente_en_rango(inicio_serie, fin_serie, fecha_inicio, fecha_fin):
    """Máscara booleana: True si el rango [inicio,fin] de cada fila se solapa
    con el rango [fecha_inicio, fecha_fin] del reporte."""
    return (
        inicio_serie.notna() & fin_serie.notna() &
        (inicio_serie <= fecha_fin) & (fin_serie >= fecha_inicio)
    )


def detectar_laboratorios(url_so, url_ct, fecha_inicio, fecha_fin):
    """
    Devuelve dict {lab_key: nombre_visible} combinando, SOLO reglas/ofertas
    vigentes en el rango [fecha_inicio, fecha_fin]:
      - Hoja1 CT (columna laboratorio), filtrando por vigencia_inicio/vigencia_fin
      - Detalle CT (columna laboratorio_det), filtrando por det_inicio/det_fin
      - SellOut: se filtran las ofertas vigentes en el rango (oferta_inicio/
        oferta_fin) y se resuelve el laboratorio de esos barcodes consultando
        Odoo (product.product.laboratory_name), ya que el Sheets de SellOut
        no trae esa columna.
    """
    labs = {}

    if url_ct and url_ct.startswith("https://"):
        df_h1 = obtener_ct_hoja1(url_ct)
        if not df_h1.empty:
            df_h1_vig = df_h1[_vigente_en_rango(df_h1['vigencia_inicio'], df_h1['vigencia_fin'], fecha_inicio, fecha_fin)]
            for _, row in df_h1_vig[['laboratorio', 'laboratorio_key']].drop_duplicates().iterrows():
                labs.setdefault(row['laboratorio_key'], row['laboratorio'].strip())
            n_desc = len(df_h1) - len(df_h1_vig)
            if n_desc > 0:
                st.caption(f"ℹ️ Hoja1 CT: {n_desc} regla(s) fuera del rango de fechas, ignoradas para detección de laboratorios.")

        df_det = obtener_ct_detalle(url_ct)
        if not df_det.empty:
            df_det_vig = df_det[_vigente_en_rango(df_det['det_inicio'], df_det['det_fin'], fecha_inicio, fecha_fin)]
            for _, row in df_det_vig[['laboratorio_det', 'laboratorio_det_key']].drop_duplicates().iterrows():
                labs.setdefault(row['laboratorio_det_key'], row['laboratorio_det'].strip())
            n_desc = len(df_det) - len(df_det_vig)
            if n_desc > 0:
                st.caption(f"ℹ️ Detalle CT: {n_desc} regla(s) fuera del rango de fechas, ignoradas para detección de laboratorios.")

    if url_so and url_so.startswith("https://"):
        df_so = obtener_ofertas_sheets(url_so)
        if not df_so.empty:
            df_so_vig = df_so[_vigente_en_rango(df_so['oferta_inicio'], df_so['oferta_fin'], fecha_inicio, fecha_fin)]
            n_desc = len(df_so) - len(df_so_vig)
            if n_desc > 0:
                st.caption(f"ℹ️ SellOut: {n_desc} oferta(s) fuera del rango de fechas, ignoradas para detección de laboratorios.")
            if not df_so_vig.empty:
                try:
                    barcodes = df_so_vig['barcode_key'].unique().tolist()
                    data = odoo_search_read(
                        'product.product', [('barcode', 'in', barcodes)], ['laboratory_name']
                    )
                    for p in data:
                        lab = p.get('laboratory_name')
                        if isinstance(lab, (list, tuple)):
                            nombre = str(lab[1]).strip()
                            labs.setdefault(nombre.lower(), nombre)
                except Exception as e:
                    st.warning(
                        f"⚠️ No se pudieron resolver laboratorios de SellOut vía Odoo tras "
                        f"{REINTENTOS_ODOO} intento(s): {e}\n\n"
                        "Puede ser una caída temporal del servidor de Odoo (503 Service Unavailable). "
                        "Podés reintentar en unos segundos con el botón 'Detectar Laboratorios', o continuar: "
                        "los laboratorios que solo provienen de SellOut no tendrán casilla configurable y "
                        "se procesarán en modo Libre por defecto al generar el reporte."
                    )

    return dict(sorted(labs.items(), key=lambda kv: kv[1].lower()))


# ─────────────────────────────────────────────
# MATCHEO CT (Hoja1 + Detalle) — sin distinción de tipo de reporte:
# se evalúan simultáneamente la rama por cadena (Farmago/Farmatención) y la
# rama general (por cliente), igual que Detalle evalúa cadena y cliente.
# ─────────────────────────────────────────────

def _filtrar_vigencia(df_m, col_inicio, col_fin, col_fecha='invoice_date_obj'):
    if col_inicio not in df_m.columns or df_m.empty:
        return df_m.iloc[0:0]
    df_m = df_m.copy()
    inicio = pd.to_datetime(df_m[col_inicio], errors='coerce').dt.date
    fin    = pd.to_datetime(df_m[col_fin],    errors='coerce').dt.date
    mask = inicio.notna() & fin.notna() & (df_m[col_fecha] >= inicio) & (df_m[col_fecha] <= fin)
    return df_m[mask]


def matchear_ct(df_final, url_ct):
    """
    Devuelve DataFrame con columnas ['id', 'descuento_ct', 'fuente_ct']
    solo para las líneas de df_final que matchean alguna regla CT vigente
    (Hoja1 y/o Detalle). Prioridad: Detalle > Hoja1; dentro de Hoja1,
    cadena (Farmago/Farmatención) > cliente general; dentro de Detalle,
    cadena > cliente.
    """
    df_hoja1   = obtener_ct_hoja1(url_ct)
    df_detalle = obtener_ct_detalle(url_ct)
    df_excluir = obtener_excluidos_ct(url_ct)

    if df_hoja1.empty and df_detalle.empty:
        return pd.DataFrame(columns=['id', 'descuento_ct', 'fuente_ct'])

    df = df_final.copy()
    df['partner_key']  = df['partner_id'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower())
    df['lab_key'] = df['laboratory_name'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower())
    df['cadena_key_f'] = df['cadena_val'].apply(
        lambda x: quitar_tildes((x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower()) if x else '')
    df['barcode_norm'] = estandarizar_barcodes(df['barcode'].apply(lambda x: x if isinstance(x, str) else str(x)))
    df['invoice_date_obj'] = pd.to_datetime(df['invoice_date'], errors='coerce').dt.date

    # ── Hoja1: rama cadena + rama general, con prioridad cadena > general ──
    hoja1_matches = []
    if not df_hoja1.empty:
        h1_cad = df_hoja1[df_hoja1['cadena_key'].isin(CADENAS_FARMAGO_FARMATENCION)]
        if not h1_cad.empty:
            # NO deduplicar antes de filtrar vigencia: puede haber múltiples
            # reglas para la misma cadena+laboratorio con rangos de fecha
            # distintos (ej. 5% del 1-8 de julio y 10% del 9-10 de julio).
            # Deduplicar aquí perdería una de las reglas antes de poder
            # evaluar cuál aplica a cada factura según su fecha real.
            m = df.merge(
                h1_cad[['cadena_key', 'laboratorio_key', 'descuento_ct', 'vigencia_inicio', 'vigencia_fin']],
                left_on=['cadena_key_f', 'lab_key'], right_on=['cadena_key', 'laboratorio_key'], how='inner'
            )
            m = _filtrar_vigencia(m, 'vigencia_inicio', 'vigencia_fin')

            # Red de seguridad: si por error de carga en el Sheets dos rangos
            # de vigencia se solapan genuinamente (misma factura matchea dos
            # reglas vigentes a la vez), priorizar el mayor descuento para no
            # duplicar la línea. Con rangos correctamente secuenciales (sin
            # solaparse) esto nunca debería activarse.
            if 'id' in m.columns and m.duplicated(subset=['id']).any():
                n_antes_cad = len(m)
                m = (
                    m.sort_values('descuento_ct', ascending=False)
                    .drop_duplicates(subset=['id'], keep='first')
                )
                n_dedup_cad = n_antes_cad - len(m)
                if n_dedup_cad > 0:
                    st.warning(
                        f"⚠️ Hoja1 CT (Farmago/Farmatención): {n_dedup_cad} línea(s) con rangos "
                        f"de vigencia solapados para la misma cadena+laboratorio — se tomó el "
                        f"mayor descuento. Revisa el Sheets si esto no era esperado."
                    )

            if not m.empty:
                m['tipo_ct'] = m['cadena_key']  # 'farmago' o 'farmatencion'
                hoja1_matches.append(m)

        h1_gen = df_hoja1[~df_hoja1['cadena_key'].isin(CADENAS_FARMAGO_FARMATENCION)]
        if not h1_gen.empty:
            m = df.merge(
                h1_gen[['partner_name_key', 'laboratorio_key', 'descuento_ct', 'vigencia_inicio', 'vigencia_fin']],
                left_on=['partner_key', 'lab_key'], right_on=['partner_name_key', 'laboratorio_key'], how='inner'
            )
            m = _filtrar_vigencia(m, 'vigencia_inicio', 'vigencia_fin')
            if not m.empty:
                m['tipo_ct'] = 'ct_lineal'
                hoja1_matches.append(m)

    df_hoja1_result = pd.DataFrame()
    if hoja1_matches:
        df_hoja1_result = pd.concat(hoja1_matches, ignore_index=True)
        if 'id' in df_hoja1_result.columns:
            df_hoja1_result = df_hoja1_result.drop_duplicates(subset=['id'], keep='first')  # cadena gana
        df_hoja1_result['_fuente'] = 'hoja1'

    # ── Detalle: por cadena + por cliente, con prioridad cadena > cliente ──
    df_detalle_result = pd.DataFrame()
    if not df_detalle.empty:
        cols_det = ['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key',
                    'descuento_det', 'det_inicio', 'det_fin']
        m_cad = df.merge(
            df_detalle[cols_det],
            left_on=['barcode_norm', 'lab_key', 'cadena_key_f'],
            right_on=['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key'], how='inner'
        )
        m_cli = df.merge(
            df_detalle[cols_det],
            left_on=['barcode_norm', 'lab_key', 'partner_key'],
            right_on=['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key'], how='inner'
        )
        m_cad = _filtrar_vigencia(m_cad, 'det_inicio', 'det_fin')
        m_cli = _filtrar_vigencia(m_cli, 'det_inicio', 'det_fin')
        m_cad = m_cad.rename(columns={'descuento_det': 'descuento_ct'})
        m_cli = m_cli.rename(columns={'descuento_det': 'descuento_ct'})

        # tipo_ct: si la cadena_cliente_det_key es farmago/farmatencion, esa es
        # la clasificación; si es un nombre de cliente/cadena distinto, se
        # considera parte del universo general (CT Lineal), igual que en los
        # módulos originales.
        for _m in (m_cad, m_cli):
            if not _m.empty:
                _m['tipo_ct'] = _m['cadena_cliente_det_key'].apply(
                    lambda v: v if v in CADENAS_FARMAGO_FARMATENCION else 'ct_lineal'
                )

        partes = [p for p in [m_cad, m_cli] if not p.empty]
        if partes:
            df_detalle_result = pd.concat(partes, ignore_index=True)
            if 'id' in df_detalle_result.columns:
                df_detalle_result = df_detalle_result.drop_duplicates(subset=['id'], keep='first')  # cadena gana
            df_detalle_result['_fuente'] = 'detalle'

    if df_hoja1_result.empty and df_detalle_result.empty:
        return pd.DataFrame(columns=['id', 'descuento_ct', 'fuente_ct', 'tipo_ct'])

    # Unión final: Hoja1 primero, Detalle al final -> keep='last' hace que Detalle gane
    partes_finales = [p for p in [df_hoja1_result, df_detalle_result] if not p.empty]
    df_union = pd.concat(partes_finales, ignore_index=True)
    if 'id' in df_union.columns:
        df_union = df_union.drop_duplicates(subset=['id'], keep='last')

    n_hoja1   = (df_union['_fuente'] == 'hoja1').sum()
    n_detalle = (df_union['_fuente'] == 'detalle').sum()
    st.toast(f"CT: {n_hoja1} desde Hoja1 · {n_detalle} desde Detalle")

    # ── Aplicar lista Excluir (solo sobre filas provenientes de Hoja1) ──
    if not df_excluir.empty and 'barcode_norm' in df_union.columns:
        mask_h1  = df_union['_fuente'] == 'hoja1'
        df_h1r   = df_union[mask_h1].copy()
        df_otros = df_union[~mask_h1].copy()
        if not df_h1r.empty:
            chk = df_h1r[['barcode_norm', 'lab_key']].merge(
                df_excluir, left_on=['barcode_norm', 'lab_key'],
                right_on=['barcode_excluido', 'laboratorio_excluir_key'], how='left', indicator=True
            )
            mask_excl = (chk['_merge'] == 'both').values
            n_excl = mask_excl.sum()
            if n_excl > 0:
                st.caption(f"ℹ️ {n_excl} línea(s) excluidas de Hoja1 por lista CT (Detalle mantiene su override).")
            df_h1r = df_h1r[~mask_excl]
        df_union = pd.concat([df_h1r, df_otros], ignore_index=True)

    df_union = df_union.rename(columns={'_fuente': 'fuente_ct'})
    return df_union[['id', 'descuento_ct', 'fuente_ct', 'tipo_ct']].drop_duplicates(subset=['id'])


def matchear_sellout(df_final, df_so):
    """
    Devuelve DataFrame con columnas ['id', 'descuento_so'] solo para líneas
    de df_final cuyo barcode tiene una oferta SellOut vigente en la fecha
    de la factura, y que además pasan el filtro de la columna 'aplica_cliente'
    del Sheets (independientes / Farmago / todos) — misma lógica que
    modulo_general_libre.
    """
    if df_so.empty:
        return pd.DataFrame(columns=['id', 'descuento_so'])

    df = df_final.copy()
    df['barcode_norm'] = estandarizar_barcodes(df['barcode'].apply(lambda x: x if isinstance(x, str) else str(x)))
    df['invoice_date_obj'] = pd.to_datetime(df['invoice_date'], errors='coerce').dt.date

    cols_so = ['barcode_key', 'descuento_so', 'oferta_inicio', 'oferta_fin']
    if 'aplica_cliente' in df_so.columns:
        cols_so.append('aplica_cliente')

    m = df.merge(
        df_so[cols_so],
        left_on='barcode_norm', right_on='barcode_key', how='inner'
    )
    mask = (
        m['oferta_inicio'].notna() & m['oferta_fin'].notna() &
        (m['invoice_date_obj'] >= m['oferta_inicio']) & (m['invoice_date_obj'] <= m['oferta_fin'])
    )
    m = m[mask]

    if 'aplica_cliente' in m.columns:
        m = filtrar_por_aplica_cliente(m)

    if 'id' not in m.columns:
        return pd.DataFrame(columns=['id', 'descuento_so'])
    return m[['id', 'descuento_so']].drop_duplicates(subset=['id'])


# ─────────────────────────────────────────────
# LÓGICA POR LABORATORIO (Libre / Excluir) + A Costo
# ─────────────────────────────────────────────

def calcular_descuentos_finales(df_bruto, config_lab):
    """
    
    df_bruto: df_final + columnas 'descuento_ct' y 'descuento_so' (NaN si no matchea).
    Ya viene filtrado a solo líneas que matchean al menos una fuente.

    Devuelve un DataFrame (puede tener MÁS filas que df_bruto) con columnas:
      - descuento_valor : descuento a aplicar en esa línea
      - fuente_final    : 'ct' o 'so' (determina la fórmula de precio unitario)
      - price_unit_base : precio base (siempre el precio de factura; ya no se
                           recalcula/ajusta entre fuentes)
      - gano_sellout    : True si la línea corresponde al lado SellOut de una
                           venta que también matcheó CT (informativo)

    Reglas por línea (según config_lab[lab]['excluir']):
      - Solo matchea CT  → una línea, descuento CT.
      - Solo matchea SO  → una línea, descuento SellOut.
      - Matchea ambas y el laboratorio NO tiene "Excluir" marcado (modo Libre):
        → SE GENERAN DOS LÍNEAS (ambas ventas), una con el descuento SellOut
          y otra con el descuento CT, cada una con precio normal (sin ajustes).
      - Matchea ambas y el laboratorio SÍ tiene "Excluir" marcado:
        → una sola línea, con el mayor de los dos descuentos.
    """
    df = df_bruto.copy()
    df['lab_key'] = df['laboratory_name'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower())

    tiene_ct = df['descuento_ct'].notna()
    tiene_so = df['descuento_so'].notna()

    solo_ct = df[tiene_ct & ~tiene_so].copy()
    solo_so = df[tiene_so & ~tiene_ct].copy()
    ambos   = df[tiene_ct & tiene_so].copy()

    partes = []

    if not solo_ct.empty:
        solo_ct['descuento_valor'] = solo_ct['descuento_ct']
        solo_ct['fuente_final']    = 'ct'
        solo_ct['tipo_reporte']    = solo_ct['tipo_ct']
        solo_ct['price_unit_base'] = solo_ct['price_unit']
        solo_ct['gano_sellout']    = False
        partes.append(solo_ct)

    if not solo_so.empty:
        solo_so['descuento_valor'] = solo_so['descuento_so']
        solo_so['fuente_final']    = 'so'
        solo_so['tipo_reporte']    = 'sellout'
        solo_so['price_unit_base'] = solo_so['price_unit']
        solo_so['gano_sellout']    = False
        partes.append(solo_so)

    if not ambos.empty:
        ambos['excluir_lab'] = ambos['lab_key'].apply(
            lambda k: config_lab.get(k, {'excluir': False, 'costo': False}).get('excluir', False)
        )

        # ── NUEVO: precio base común, siempre recalculado desde SellOut ──
        # price_unit / (1 - descuento_so). Se usa en la línea CT (modo Libre)
        # y en la línea ganadora (modo Excluir), tal como indica el comentario
        # inicial del módulo: "recalculando el precio base desde SellOut".
        ambos['price_unit_base_so'] = (
            ambos['price_unit'] / (1 - ambos['descuento_so'].clip(upper=0.9999))
        )

        # ── Modo Excluir: una sola línea, con el mayor descuento ──
        ambos_excl = ambos[ambos['excluir_lab']].copy()
        if not ambos_excl.empty:
            so_gana = ambos_excl['descuento_so'] >= ambos_excl['descuento_ct']
            ambos_excl['descuento_valor'] = np.where(so_gana, ambos_excl['descuento_so'], ambos_excl['descuento_ct'])
            ambos_excl['fuente_final']    = np.where(so_gana, 'so', 'ct')
            ambos_excl['tipo_reporte']    = np.where(so_gana, 'sellout', ambos_excl['tipo_ct'])
            ambos_excl['price_unit_base'] = ambos_excl['price_unit_base_so']
            ambos_excl['gano_sellout']    = so_gana
            partes.append(ambos_excl.drop(columns=['excluir_lab', 'price_unit_base_so']))

        

        # ── Modo Libre: DOS líneas — ambas ventas se reportan ──
        ambos_libre = ambos[~ambos['excluir_lab']].copy()
        if not ambos_libre.empty:
            linea_so = ambos_libre.drop(columns=['excluir_lab']).copy()
            linea_so['descuento_valor'] = linea_so['descuento_so']
            linea_so['fuente_final']    = 'so'
            linea_so['tipo_reporte']    = 'sellout'
            linea_so['price_unit_base'] = linea_so['price_unit']
            linea_so['gano_sellout']    = True

            linea_ct = ambos_libre.drop(columns=['excluir_lab']).copy()
            linea_ct['descuento_valor'] = linea_ct['descuento_ct']
            linea_ct['fuente_final']    = 'ct'
            linea_ct['tipo_reporte']    = linea_ct['tipo_ct']
            linea_ct['price_unit_base'] = linea_ct['price_unit_base_so']
            linea_ct['gano_sellout']    = False

            partes.append(linea_so.drop(columns=['price_unit_base_so']))
            partes.append(linea_ct.drop(columns=['price_unit_base_so']))

    if not partes:
        cols = list(df.columns) + ['descuento_valor', 'fuente_final', 'tipo_reporte', 'price_unit_base', 'gano_sellout']
        return pd.DataFrame(columns=cols)

    resultado = pd.concat(partes, ignore_index=True)
    resultado = resultado.drop(columns=['descuento_ct', 'descuento_so', 'tipo_ct'], errors='ignore')
    return resultado


# ─────────────────────────────────────────────
# MOTOR DE EXCEL — un único archivo con todos los laboratorios
# ─────────────────────────────────────────────

def calcular_valor_unitario(row, config_lab):
    cfg = config_lab.get(row['lab_key'], {'excluir': False, 'costo': False})
    if cfg.get('costo', False):
        return row['costo_laboratorio']
    if row['fuente_final'] == 'so':
        d = row['descuento_valor']
        if d >= 1 or d < 0:
            return row['price_unit']
        return row['price_unit'] / (1 - d)
    return row['price_unit_base']


def _es_moneda_usd(moneda):
    m = str(moneda).lower()
    return any(token in m for token in ['usd', 'dolar', 'dólar', '$'])


def _escribir_hoja_reporte(workbook, sheet_name, reporte, header_format, percent_format,
                            num_format_moneda, bold_format):
    """Escribe una hoja de reporte homogénea en moneda, con el mismo layout/fórmulas
    que los módulos originales."""
    worksheet = workbook.add_worksheet(sheet_name)
    monto_format      = workbook.add_format({'num_format': num_format_moneda})
    bold_monto_format = workbook.add_format({'num_format': num_format_moneda, 'bold': True})

    worksheet.set_column(10, 10, None, percent_format)
    encabezados = [
        'Fecha Factura', 'ID Cliente', 'Cliente', 'Nro. Factura',
        'Código de Barras', 'Descripción', 'Laboratorio', 'Código Laboratorio',
        'Cantidad', 'Precio', 'Descuento %', 'Total', 'Monto NC'
    ]
    for col_num, value in enumerate(encabezados):
        worksheet.write(0, col_num, value, header_format)

    columnas_datos = [
        'invoice_date', 'partner_id_num', 'partner_id', 'invoice_number_next',
        'barcode', 'name', 'laboratory_name', 'supplier_code',
        'quantity', 'valor_unitario', 'descuento',
    ]
    for row_num, (_, fila) in enumerate(reporte.iterrows()):
        for col_num, campo in enumerate(columnas_datos):
            worksheet.write(row_num + 1, col_num, fila[campo])
        worksheet.write(row_num + 1, 9, fila['valor_unitario'], monto_format)
        worksheet.write_formula(row_num + 1, 11, f'=J{row_num + 2}*I{row_num + 2}', monto_format)
        worksheet.write_formula(row_num + 1, 12, f'=K{row_num + 2}*L{row_num + 2}', monto_format)

    last_row = len(reporte) + 1
    worksheet.write(last_row, 11, "Total NC", bold_format)
    worksheet.write_formula(last_row, 12, f"=SUM(M2:M{last_row})", bold_monto_format)

    for i, col in enumerate(columnas_datos):
        if i < 10:
            col_data = reporte[col].astype(str).fillna("")
            worksheet.set_column(i, i, max(col_data.map(len).max(), len(encabezados[i])) + 2)
    subtotal = reporte['quantity'] * reporte['valor_unitario']
    total_desc = subtotal * reporte['descuento']
    worksheet.set_column(11, 11, max(subtotal.astype(str).map(len).max(), 12) + 6)
    worksheet.set_column(12, 12, max(total_desc.astype(str).map(len).max(), 12) + 6)

    return worksheet


def generar_excel_unico(df_res, config_lab):
    """
    Genera un único Excel con TODOS los laboratorios, separado en dos
    pestañas según la moneda del producto:
      - "Ventas USD": líneas cuya moneda es dólares.
      - "Ventas Bs":  líneas cuya moneda es bolívares (o cualquier otra
        distinta de USD).
    Si alguno de los dos grupos no tiene líneas, esa pestaña no se crea.
    """
    if df_res.empty:
        return None

    df = df_res.copy()
    for col in ['quantity', 'price_unit', 'costo_laboratorio', 'descuento_valor', 'price_unit_base']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['valor_calculado'] = df.apply(lambda r: calcular_valor_unitario(r, config_lab), axis=1)
    df['es_usd'] = df['currency_id'].apply(_es_moneda_usd)

    reporte = pd.DataFrame({
        'invoice_date':        df['invoice_date'],
        'partner_id_num':      df['partner_id_num'],
        'partner_id':          df['partner_id'],
        'invoice_number_next': df['invoice_number_next'],
        'barcode':             df['barcode'],
        'name':                df['name'],
        'laboratory_name':     df['laboratory_name'],
        'supplier_code':       df['supplier_code'],
        'quantity':            df['quantity'],
        'valor_unitario':      df['valor_calculado'],
        'descuento':           df['descuento_valor'],
        'es_usd':              df['es_usd'],
    })
    reporte_usd = reporte[reporte['es_usd']].drop(columns=['es_usd']).reset_index(drop=True)
    reporte_bs  = reporte[~reporte['es_usd']].drop(columns=['es_usd']).reset_index(drop=True)

    if reporte_usd.empty and reporte_bs.empty:
        return None

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        header_format  = workbook.add_format({'bold': True, 'border': 0})
        percent_format = workbook.add_format({'num_format': '0%'})
        bold_format    = workbook.add_format({'bold': True})

        if not reporte_usd.empty:
            _escribir_hoja_reporte(
                workbook, "Ventas USD", reporte_usd,
                header_format, percent_format, '$#,##0.00', bold_format
            )
        if not reporte_bs.empty:
            _escribir_hoja_reporte(
                workbook, "Ventas Bs", reporte_bs,
                header_format, percent_format, '"Bs." #,##0.00', bold_format
            )

    output.seek(0)
    return output.getvalue()


# ─────────────────────────────────────────────
# EXCEL INDIVIDUAL POR LAB
# ─────────────────────────────────────────────
def generar_excel_lab_tipo(df_lab_tipo, config_lab):
    """
    Genera un Excel de una sola hoja para un laboratorio y tipo de reporte
    específico (SellOut / CT Lineal / Farmago / Farmatención), replicando
    el formato de los módulos individuales (una fila por línea, formato
    de moneda condicional por fila según currency_id).
    """
    if df_lab_tipo.empty:
        return None

    df = df_lab_tipo.copy()
    for col in ['quantity', 'price_unit', 'costo_laboratorio', 'descuento_valor', 'price_unit_base']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['valor_calculado'] = df.apply(lambda r: calcular_valor_unitario(r, config_lab), axis=1)

    reporte = pd.DataFrame({
        'invoice_date':        df['invoice_date'],
        'partner_id_num':      df['partner_id_num'],
        'partner_id':          df['partner_id'],
        'invoice_number_next': df['invoice_number_next'],
        'barcode':             df['barcode'],
        'name':                df['name'],
        'laboratory_name':     df['laboratory_name'],
        'supplier_code':       df['supplier_code'],
        'quantity':            df['quantity'],
        'valor_unitario':      df['valor_calculado'],
        'descuento':           df['descuento_valor'],
        'Moneda':              df['currency_id'],
    })
    reporte['subtotal_bruto']  = reporte['quantity'] * reporte['valor_unitario']
    reporte['total_descuento'] = reporte['subtotal_bruto'] * reporte['descuento']

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        reporte_export = reporte.drop(columns=['Moneda'], errors='ignore')
        reporte_export.to_excel(writer, index=False, sheet_name='Reporte', startrow=1, header=False)
        workbook  = writer.book
        worksheet = writer.sheets['Reporte']

        header_format  = workbook.add_format({'bold': True, 'border': 0})
        percent_format = workbook.add_format({'num_format': '0%'})
        dollar_format  = workbook.add_format({'num_format': '$#,##0.00'})
        bs_format      = workbook.add_format({'num_format': '"Bs." #,##0.00'})
        bold_format    = workbook.add_format({'bold': True})

        worksheet.set_column(10, 10, None, percent_format)
        encabezados = [
            'Fecha Factura', 'ID Cliente', 'Cliente', 'Nro. Factura',
            'Código de Barras', 'Descripción', 'Laboratorio', 'Código Laboratorio',
            'Cantidad', 'Precio', 'Descuento %', 'Total', 'Monto NC'
        ]
        for col_num, value in enumerate(encabezados):
            worksheet.write(0, col_num, value, header_format)

        for row_num in range(len(reporte)):
            worksheet.write_formula(row_num + 1, 11, f'=J{row_num + 2}*I{row_num + 2}')
            worksheet.write_formula(row_num + 1, 12, f'=K{row_num + 2}*L{row_num + 2}')

        last_row = len(reporte) + 1
        worksheet.write(last_row, 11, "Total NC", bold_format)
        worksheet.write_formula(last_row, 12, f"=SUM(M2:M{last_row})", bold_format)

        fmt = bs_format
        for row_num, moneda in enumerate(reporte["Moneda"], start=1):
            fmt = dollar_format if str(moneda).lower() in ["usd", "dolares", "$"] else bs_format
            worksheet.conditional_format(row_num, 9,  row_num, 9,  {'type': 'no_errors', 'format': fmt})
            worksheet.conditional_format(row_num, 11, row_num, 11, {'type': 'no_errors', 'format': fmt})
            worksheet.conditional_format(row_num, 12, row_num, 12, {'type': 'no_errors', 'format': fmt})
        worksheet.conditional_format(last_row, 12, last_row, 12, {'type': 'no_errors', 'format': fmt})

        for i, col in enumerate(reporte.columns):
            if i < 10:
                col_data = reporte[col].astype(str).fillna("")
                worksheet.set_column(i, i, max(col_data.map(len).max(), len(col)) + 2)
        for col_idx in [11, 12]:
            col_data = reporte['subtotal_bruto' if col_idx == 11 else 'total_descuento'].fillna(0).astype(float)
            worksheet.set_column(col_idx, col_idx, max(col_data.astype(str).map(len).max(), 12) + 6)

    output.seek(0)
    return output.getvalue()


# ─────────────────────────────────────────────
# ENVIAR A SHEETS - LAB + TIPO
# ─────────────────────────────────────────────

def enviar_a_sheets_consolidado(df_res, fecha_inicio, fecha_fin, apps_script_url, config_lab):
    """
    Replica el envío de resumen a Google Sheets de los módulos originales,
    iterando sobre cada combinación (laboratorio, tipo_reporte) presente en
    el consolidado. Cada combinación se envía como un registro independiente,
    con el mismo 'concepto' que generaría el módulo original correspondiente,
    y respetando 'A Costo' según la configuración del laboratorio.
    """
    if df_res.empty:
        return [], []

    df = df_res.copy()
    for col in ['quantity', 'price_unit', 'costo_laboratorio', 'descuento_valor', 'price_unit_base']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['valor_calculado'] = df.apply(lambda r: calcular_valor_unitario(r, config_lab), axis=1)
    df['subtotal_bruto']  = df['quantity'] * df['valor_calculado']
    df['total_descuento'] = df['subtotal_bruto'] * df['descuento_valor']

    meses_es = {
        'January':'Enero','February':'Febrero','March':'Marzo','April':'Abril',
        'May':'Mayo','June':'Junio','July':'Julio','August':'Agosto',
        'September':'Septiembre','October':'Octubre','November':'Noviembre','December':'Diciembre'
    }
    mes = f"{meses_es.get(fecha_inicio.strftime('%B'), fecha_inicio.strftime('%B'))} {fecha_inicio.strftime('%Y')}"

    resultados, errores = [], []
    for (lab, tipo), grupo in df.groupby(['laboratory_name', 'tipo_reporte']):
        etiqueta_tipo = TIPO_LABELS.get(tipo, tipo)
        if tipo == 'sellout':
            concepto = f"Sell-Out del {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')} (en Panel)"
        else:
            concepto = f"{etiqueta_tipo} del {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}"

        moneda = str(grupo['currency_id'].iloc[0]).lower().strip()
        total  = grupo['total_descuento'].sum()
        es_usd = any(m in moneda for m in ['usd', 'dolar', '$'])

        payload = {
            "action": "append_data",
            "data": {
                "laboratorio": lab, "mes": mes, "concepto": concepto,
                "monto_bs":  0 if es_usd else round(total, 2),
                "monto_usd": round(total, 2) if es_usd else 0,
            }
        }
        try:
            result = requests.post(apps_script_url, json=payload, timeout=15).json()
            if result.get("success"):
                resultados.append(f"{lab} — {etiqueta_tipo}")
            else:
                errores.append(f"{lab} — {etiqueta_tipo}: {result.get('error', 'error desconocido')}")
        except Exception as ex:
            errores.append(f"{lab} — {etiqueta_tipo}: {str(ex)}")

    return resultados, errores

# ─────────────────────────────────────────────
# ARMADO CORREOS
# ─────────────────────────────────────────────
def extraer_correos_html(html):
    import re
    if not html:
        return []
    return list(set(re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', html)))

ORDEN_TIPOS = ['sellout', 'ct_lineal', 'farmago', 'farmatencion']

def _unir_tipos_es(items):
    """['A'] -> 'A'; ['A','B'] -> 'A y B'; ['A','B','C'] -> 'A, B y C'."""
    items = list(items)
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " y " + items[-1]

def generar_mailto_consolidado(lab, tipos_presentes, fecha_inicio, fecha_fin, comment_por_lab, cc_emails):
    """
    Arma el mailto para un laboratorio del consolidado. A diferencia de los
    módulos individuales (un solo tipo_activo por sesión), aquí un mismo
    laboratorio puede tener líneas en varios tipos de reporte a la vez
    (SellOut, CT Lineal, Farmago, Farmatención). El asunto se arma según
    cuántos tipos aplican:
      - 1 tipo  -> "Reporte SELL-OUT del ..."
      - 2+ tipos -> "Reportes SELL-OUT, Farmago y Descuentos CT Lineal del ..."
    Cuerpo y regla especial de Leti se mantienen igual que en los módulos originales.
    """
    correos_to = extraer_correos_html(comment_por_lab.get(lab, ''))

    tipos_ordenados = [t for t in ORDEN_TIPOS if t in tipos_presentes]
    etiquetas = [TIPO_LABELS.get(t, t) for t in tipos_ordenados]

    if len(etiquetas) <= 1:
        texto_reporte = f"Reporte {etiquetas[0]}" if etiquetas else "Reporte"
    else:
        texto_reporte = f"Reportes {_unir_tipos_es(etiquetas)}"

    asunto = (
        f"{lab} | {texto_reporte} del "
        f"{fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}"
    )

    cuerpo = (
        "Estimados señores, espero se encuentren bien.\n\n"
        "Se envían los reportes con los descuentos aprobados durante el período indicado "
        "para su reconocimiento a través de la nota de crédito correspondiente.\n\n"
    )
    if 'leti' in lab.lower():
        cuerpo += (
            "de no recibirse la nota en 5 días y habiendo cuentas por pagar vigentes, "
            "se procederá a rebajarse el monto correspondiente en el pago.\n\n"
        )
    cuerpo += "Saludos,"

    mailto = (
        f"mailto:{urllib.parse.quote(','.join(correos_to))}"
        f"?cc={urllib.parse.quote(','.join(cc_emails))}"
        f"&subject={urllib.parse.quote(asunto)}"
        f"&body={urllib.parse.quote(cuerpo)}"
    )
    return mailto, correos_to


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def render_reporte(fecha_inicio, fecha_fin):
    st.header("🎯 Panel de Reportes Unificado")

    def limpiar(val):
        return val[1] if isinstance(val, (list, tuple)) else val

    def limpiar_barcode(val):
        if not val:
            return ""
        return str(val).split('.')[0].strip()

    for key, default in [
        ('df_resultado', None), ('excel_binario', None),
        ('labs_detectados', {}), ('config_lab', {}), ('df_bruto', None),
        ('_odoo_client', None),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    # ── 1. FUENTES DE DATOS ─────────────────────────────────────────
    st.subheader("1️⃣ Fuentes de datos")
    col1, col2 = st.columns(2)
    with col1:
        url_so = st.text_input("Link de Google Sheets (SellOut)", URL_SO_DEFAULT, key="url_so_input")
    with col2:
        url_ct = st.text_input("Link de Google Sheets (CT Fijo)", URL_CT_DEFAULT, key="url_ct_input")

    st.caption(f"📅 Periodo del reporte: {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')} "
               "(la detección de laboratorios solo considera reglas/ofertas vigentes en este rango).")

    if st.button("🔍 Detectar Laboratorios"):
        with st.spinner("Detectando laboratorios en SellOut y CT..."):
            labs = detectar_laboratorios(url_so, url_ct, fecha_inicio, fecha_fin)
        if labs:
            st.session_state.labs_detectados = labs
            for lab_key, lab_nombre in labs.items():
                if lab_key not in st.session_state.config_lab:
                    preset = PRESET_CONFIG_LAB_NORM.get(_normalizar_nombre_lab(lab_nombre))
                    st.session_state.config_lab[lab_key] = dict(preset) if preset else {'excluir': False, 'costo': False}
            st.success(f"✅ {len(labs)} laboratorio(s) detectado(s).")
        else:
            st.warning("No se detectaron laboratorios. Revisa los links de Sheets.")

    # ── 2. CONFIGURACIÓN POR LABORATORIO ────────────────────────────
    if st.session_state.labs_detectados:
        st.divider()
        st.subheader("2️⃣ Configuración por laboratorio")
        st.caption(
            "Sin marcar nada, cada laboratorio funciona en modo **Libre**: si un producto tiene "
            "descuento vigente en SellOut y en CT a la vez, se genera una sola línea con el "
            "descuento CT (ajustando el precio base cuando SellOut es mayor). "
            "**Excluir**: si hay ambos descuentos, se conserva solo el mayor. "
            "**A Costo**: el precio unitario del Excel se reemplaza por el costo de laboratorio."
        )
        header = st.columns([3, 1, 1])
        header[0].markdown("**Laboratorio**")
        header[1].markdown("**Excluir**")
        header[2].markdown("**A Costo**")
        for lab_key, lab_nombre in st.session_state.labs_detectados.items():
            c1, c2, c3 = st.columns([3, 1, 1])
            c1.write(lab_nombre)
            cfg_actual = st.session_state.config_lab.get(lab_key, {'excluir': False, 'costo': False})
            excl  = c2.checkbox("Excluir", value=cfg_actual['excluir'], key=f"excl_{lab_key}", label_visibility="collapsed")
            costo = c3.checkbox("A Costo", value=cfg_actual['costo'],  key=f"costo_{lab_key}", label_visibility="collapsed")
            st.session_state.config_lab[lab_key] = {'excluir': excl, 'costo': costo}

        st.divider()

        # ── 3. Generar Consolidado ───────────────────────────────────────
        if st.button("🚀 Generar Consolidado", type="primary"):
            try:
                domain = [
                    ('date', '>=', str(fecha_inicio)), ('date', '<=', str(fecha_fin)),
                    ('move_type', '=', 'out_invoice'),  ('parent_state', '=', 'posted'),
                    ('move_name', 'not ilike', 'ND%'),  ('product_id', '!=', False),
                    ('quantity', '>', 0),
                ]

                with st.spinner("Consultando Odoo..."):
                    data_lineas = odoo_search_read(
                        'account.move.line', domain,
                        ['move_id', 'product_id', 'name', 'quantity', 'price_unit']
                    )
                    if not data_lineas:
                        st.warning("No hay datos para esta selección.")
                        return
                    df_lineas = pd.DataFrame(data_lineas)

                    move_ids    = list(set([x[0] for x in df_lineas['move_id']    if isinstance(x, list)]))
                    product_ids = list(set([x[0] for x in df_lineas['product_id'] if isinstance(x, list)]))

                    df_moves = pd.DataFrame(odoo_search_read(
                        'account.move', [('id', 'in', move_ids)],
                        ['invoice_date', 'partner_id', 'invoice_number_next', 'currency_id']
                    )).rename(columns={'id': 'move_id_int'})

                    # Primero traer productos (variantes) con su template asociado
                    df_prods = pd.DataFrame(odoo_search_read(
                        'product.product', [('id', 'in', product_ids), ('active', 'in', [True, False])],
                        ['laboratory_name', 'supplier_code', 'barcode', 'product_tmpl_id']
                    )).rename(columns={'id': 'product_id_int'})

                    df_prods['product_tmpl_id_int'] = df_prods['product_tmpl_id'].apply(
                        lambda x: x[0] if isinstance(x, (list, tuple)) else x
                    )

                    # Ahora sí, con los templates REALES, pedir los costos
                    tmpl_ids_reales = df_prods['product_tmpl_id_int'].dropna().unique().tolist()

                    data_costs = odoo_search_read(
                        'product.supplierinfo', [('product_tmpl_id', 'in', tmpl_ids_reales)],
                        ['product_tmpl_id', 'price']
                    )
                    if data_costs:
                        df_costs = pd.DataFrame(data_costs)
                        df_costs['product_tmpl_id_int'] = df_costs['product_tmpl_id'].apply(
                            lambda x: x[0] if isinstance(x, (list, tuple)) else x)
                        df_costs = df_costs.rename(columns={'price': 'costo_proveedor'}).drop_duplicates('product_tmpl_id_int')
                    else:
                        df_costs = pd.DataFrame(columns=['product_tmpl_id_int', 'costo_proveedor'])

                    partner_ids_raw = list(set([
                        m['partner_id'][0] for m in df_moves.to_dict('records')
                        if isinstance(m.get('partner_id'), (list, tuple))
                    ]))
                    df_partners = pd.DataFrame(odoo_search_read(
                        'res.partner', [('id', 'in', partner_ids_raw)], ['id', 'cadena']
                    )).rename(columns={'id': 'partner_id_int', 'cadena': 'cadena_val'})

                    df_lineas['move_id_int']    = df_lineas['move_id'].apply(lambda x: x[0] if isinstance(x, list) else x)
                    df_lineas['product_id_int'] = df_lineas['product_id'].apply(lambda x: x[0] if isinstance(x, list) else x)

                    df_final = df_lineas.merge(df_moves, on='move_id_int', how='left')
                    df_final = df_final.merge(df_prods, on='product_id_int', how='left')  # trae product_tmpl_id_int
                    df_final = df_final.merge(
                        df_costs[['product_tmpl_id_int', 'costo_proveedor']],
                        on='product_tmpl_id_int',   # ← cruce correcto: template contra template
                        how='left'
                    )
                    df_final['partner_id_int'] = df_final['partner_id'].apply(
                        lambda x: x[0] if isinstance(x, (list, tuple)) else None)
                    df_final = df_final.merge(df_partners, on='partner_id_int', how='left')

                    # ── Matcheo contra SellOut y CT ──────────────────────
                    df_so = obtener_ofertas_sheets(url_so) if url_so else pd.DataFrame()
                    df_so_match = matchear_sellout(df_final, df_so)
                    df_ct_match = matchear_ct(df_final, url_ct) if url_ct else pd.DataFrame(columns=['id', 'descuento_ct', 'fuente_ct', 'tipo_ct'])

                    df_bruto = df_final.merge(df_ct_match[['id', 'descuento_ct', 'tipo_ct']], on='id', how='left')
                    df_bruto = df_bruto.merge(df_so_match[['id', 'descuento_so']], on='id', how='left')
                    df_bruto = df_bruto[df_bruto['descuento_ct'].notna() | df_bruto['descuento_so'].notna()].copy()

                    if df_bruto.empty:
                        st.warning("No se encontraron líneas con descuento vigente (SellOut o CT) en el período seleccionado.")
                        return

                    # Correos por laboratorio
                    lab_names = list({
                        (x[1] if isinstance(x, (list, tuple)) else x)
                        for x in df_final['laboratory_name'].dropna() if x
                    })
                    df_lab_partners = pd.DataFrame(odoo_search_read(
                        'res.partner', [('name', 'in', lab_names)], ['id', 'name', 'comment']
                    ))
                    comment_por_lab = {}
                    for _, row in df_lab_partners.iterrows():
                        c = row['comment']
                        comment_por_lab[row['name']] = str(c).strip() if c and c is not False else ''
                    st.session_state.comment_por_lab = comment_por_lab

                    st.session_state.df_bruto = df_bruto
                    st.success(f"✅ {len(df_bruto)} línea(s) con descuento vigente encontradas.")
                    _recalcular_resultado(limpiar, limpiar_barcode)
                    st.rerun()

                    

            except Exception as e:
                st.error(
                    f"Error crítico: {e}\n\n"
                    "Si el error menciona '503' o 'Service Unavailable', es una caída temporal del "
                    "servidor de Odoo (no del reporte). Esperá unos segundos y volvé a presionar "
                    "'🚀 Generar Consolidado."
                )

    # ── 4. RENDERIZADO DE RESULTADOS ─────────────────────────────────
    if st.session_state.df_resultado is not None:
        df_display = st.session_state.df_resultado

        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("🔄 Recalcular con configuración actual"):
                _recalcular_resultado(limpiar, limpiar_barcode)
                st.toast("Reporte recalculado con la configuración actual.")
                st.rerun()
        with col2:
            if st.button("🗑️ Limpiar Todo"):
                st.session_state.df_resultado = None
                st.session_state.excel_binario = None
                st.session_state.df_bruto      = None
                st.rerun()

        st.success(f"✅ Extracción finalizada: {len(df_display)} registros.")

        cols_display = [c for c in df_display.columns if c not in ('lab_key', 'price_unit_base')]
        st.dataframe(df_display[cols_display], use_container_width=True)

        if st.session_state.excel_binario:
            st.divider()
            st.download_button(
                label="📦 Descargar Reporte (Excel)",
                data=st.session_state.excel_binario,
                file_name=f"Reporte_Unificado_del_{fecha_inicio.strftime('%d-%m-%Y')}_al_{fecha_fin.strftime('%d-%m-%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        

        # ── 3️⃣ Descargas por proveedor ──────────────────────────────
        st.divider()
        st.subheader("3️⃣ Descargas por proveedor")
        st.caption(
            "Para cada laboratorio, descarga por separado las líneas correspondientes "
            "a SellOut, CT Lineal, Farmago o Farmatención, con el mismo formato de "
            "los módulos individuales."
        )
        labs_en_resultado = sorted(df_display['laboratory_name'].unique())
        for lab in labs_en_resultado:
            lab_key_serie = df_display.loc[df_display['laboratory_name'] == lab, 'lab_key']
            lab_key = lab_key_serie.iloc[0] if not lab_key_serie.empty else lab.lower()
            with st.expander(f"📦 {lab}"):
                cols = st.columns(4)
                for idx, tipo in enumerate(['sellout', 'ct_lineal', 'farmago', 'farmatencion']):
                    etiqueta = TIPO_LABELS[tipo]
                    df_sub = df_display[
                        (df_display['laboratory_name'] == lab) &
                        (df_display['tipo_reporte'] == tipo)
                    ]
                    with cols[idx]:
                        if df_sub.empty:
                            st.caption(f"{etiqueta}\n\nsin líneas")
                        else:
                            excel_bytes = generar_excel_lab_tipo(df_sub, st.session_state.config_lab)
                            safe_lab = _slug_lab(lab)
                            st.download_button(
                                label=f"{etiqueta} ({len(df_sub)})",
                                data=excel_bytes,
                                file_name=(
                                    f"{safe_lab}_{etiqueta.replace(' ', '_')}_del_"
                                    f"{fecha_inicio.strftime('%d-%m-%Y')}_al_"
                                    f"{fecha_fin.strftime('%d-%m-%Y')}.xlsx"
                                ),
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dl_{lab_key}_{tipo}"
                            )

        # ── 4️⃣ Enviar a Google Sheets ───────────────────────────────
        st.divider()
        st.subheader("4️⃣ Enviar resumen a Google Sheets")
        apps_url = st.text_input(
            "URL del Apps Script",
            value=st.secrets.get("appscript", {}).get("url", ""),
            key="input_apps_script_url_consolidado"
        )
        if st.button("📨 Enviar resumen NC a Sheets", type="primary"):
            if not apps_url:
                st.error("Ingresa la URL del Apps Script.")
            else:
                with st.spinner("Enviando datos..."):
                    ok, errores = enviar_a_sheets_consolidado(
                        df_display, fecha_inicio, fecha_fin, apps_url, st.session_state.config_lab
                    )
                if ok:
                    st.success(f"✅ Enviados: {', '.join(ok)}")
                for e in errores:
                    st.error(f"❌ {e}")

        # ── 5️⃣ Enviar correos a laboratorios ────────────────────────────
        st.divider()
        st.subheader("5️⃣ Enviar correos a laboratorios")
        CC_DEFAULT = ["staddeo.blv@gmail.com", "staddeo@drogueriablv.com"]
        cc_input = st.text_input(
            "CC adicionales (separados por coma)",
            value=", ".join(CC_DEFAULT), key="cc_emails_input_consolidado"
        )
        CC_EMAILS = [e.strip() for e in cc_input.split(",") if e.strip()]
        comment_por_lab = st.session_state.get('comment_por_lab', {})

        labs_en_resultado = sorted(df_display['laboratory_name'].unique())
        for i in range(0, len(labs_en_resultado), 3):
            cols = st.columns(3)
            for j in range(3):
                if i + j < len(labs_en_resultado):
                    lab = labs_en_resultado[i + j]
                    tipos_lab = set(
                        df_display.loc[df_display['laboratory_name'] == lab, 'tipo_reporte'].unique()
                    )
                    mailto, correos_to = generar_mailto_consolidado(
                        lab, tipos_lab, fecha_inicio, fecha_fin, comment_por_lab, CC_EMAILS
                    )
                    tiene_correos = len(correos_to) > 0
                    etiqueta = f"✉️ {lab}" if tiene_correos else f"⚠️ {lab} (sin correos)"
                    etiquetas_tipo = ", ".join(TIPO_LABELS.get(t, t) for t in ORDEN_TIPOS if t in tipos_lab)
                    with cols[j]:
                        st.link_button(etiqueta, mailto, disabled=not tiene_correos)
                        st.caption(etiquetas_tipo)            






def _recalcular_resultado(limpiar, limpiar_barcode):
    """Aplica config_lab sobre df_bruto (sin volver a consultar Odoo) y regenera
    el DataFrame de resultado + el Excel único."""
    df_bruto = st.session_state.df_bruto
    if df_bruto is None or df_bruto.empty:
        return

    df_final = calcular_descuentos_finales(df_bruto, st.session_state.config_lab)

    res = pd.DataFrame({
        'invoice_date':        pd.to_datetime(df_final['invoice_date']).dt.strftime('%d/%m/%Y'),
        'partner_id_num':      df_final['partner_id'].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else x),
        'partner_id':          df_final['partner_id'].apply(limpiar),
        'cadena':              df_final['cadena_val'].apply(lambda x: limpiar(x) if x else ""),
        'invoice_number_next': df_final['invoice_number_next'],
        'barcode':             df_final['barcode'].apply(limpiar_barcode),
        'name':                df_final['name'],
        'laboratory_name':     df_final['laboratory_name'].apply(limpiar),
        'supplier_code':       df_final['supplier_code'].apply(lambda x: '' if x is False or x is None else str(x)),
        'quantity':            df_final['quantity'],
        'price_unit':          df_final['price_unit'],
        'costo_laboratorio':   df_final['costo_proveedor'].fillna(0),
        'descuento_valor':     df_final['descuento_valor'],
        'currency_id':         df_final['currency_id'].apply(limpiar),
        'fuente_final':        df_final['fuente_final'],
        'tipo_reporte':        df_final['tipo_reporte'],
        'gano_sellout':        df_final['gano_sellout'],
        'price_unit_base':     df_final['price_unit_base'],
        'lab_key':             df_final['lab_key'],
    })

    st.session_state.df_resultado = res
    st.session_state.excel_binario = generar_excel_unico(res, st.session_state.config_lab)
