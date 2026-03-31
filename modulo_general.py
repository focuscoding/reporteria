import streamlit as st
import pandas as pd
import io
from datetime import date
import urllib.parse
from odoo_utils import OdooClient
import numpy as np
import requests
import unicodedata

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
GID_HOJA1   = 0
GID_EXCLUIR = 1591504897
GID_DETALLE = 150387248

CADENAS_FARMAGO_FARMATENCION = {'farmago', 'farmatencion'}  # sin tildes para comparación


# ─────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────

def estandarizar_barcodes(serie):
    return serie.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

def quitar_tildes(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s))
                   if unicodedata.category(c) != 'Mn')

def limpiar_odoo(val):
    return val[1] if isinstance(val, (list, tuple)) else val

def extraer_correos_html(html):
    import re
    if not html:
        return []
    return list(set(re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', html)))

def url_con_gid(url_base, gid):
    """Construye URL CSV para una pestaña específica del Sheets."""
    base = url_base.split('?')[0]
    return f"{base}?format=csv&gid={gid}"


# ─────────────────────────────────────────────
# LECTURA DE SHEETS
# ─────────────────────────────────────────────

def obtener_ofertas_sheets(url):
    """
    Sheets SellOut: A=barcode, E=descuento, F=nc_check, H=inicio, I=fin.
    Solo filas NC. Devuelve descuento_so normalizado.
    """
    try:
        df = pd.read_csv(url)
        df = df.rename(columns={
            df.columns[0]: 'barcode_key',
            df.columns[4]: 'descuento_valor',
            df.columns[5]: 'nc_check',
            df.columns[7]: 'oferta_inicio',
            df.columns[8]: 'oferta_fin',
        })
        df['nc_check']    = df['nc_check'].astype(str).str.upper().str.strip()
        df                = df[df['nc_check'] == 'NC'].copy()
        df['barcode_key'] = estandarizar_barcodes(df['barcode_key'])
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


def obtener_ct_hoja1(url_base):
    """
    Pestaña Hoja1 (gid=0): A=partner_name, B=cadena, C=laboratorio, D=descuento(%)
    Descarta filas con descuento no numérico.
    Devuelve descuento_ct normalizado + claves de merge.
    """
    try:
        df = pd.read_csv(url_con_gid(url_base, GID_HOJA1))
        df = df.rename(columns={
            df.columns[0]: 'partner_name',
            df.columns[1]: 'cadena',
            df.columns[2]: 'laboratorio',
            df.columns[3]: 'descuento_valor_raw',
        })

        desc_limpio   = df['descuento_valor_raw'].astype(str).str.replace('%','',regex=False).str.strip()
        filas_validas = pd.to_numeric(desc_limpio, errors='coerce').notna()
        descartadas   = (~filas_validas).sum()
        if descartadas > 0:
            st.caption(f"⚠️ Hoja1 CT: {descartadas} fila(s) con descuento no numérico ignoradas.")
        df = df[filas_validas].copy()

        df['descuento_ct']     = desc_limpio[filas_validas].astype(float) / 100
        df['partner_name_key'] = df['partner_name'].astype(str).str.strip().str.lower()
        df['laboratorio_key']  = df['laboratorio'].astype(str).str.strip().str.lower()
        df['cadena_key']       = df['cadena'].astype(str).str.strip().str.lower().apply(quitar_tildes)

        return df[['partner_name', 'partner_name_key', 'cadena', 'cadena_key',
                   'laboratorio', 'laboratorio_key', 'descuento_ct']]
    except Exception as e:
        st.error(f"Error leyendo Hoja1 CT: {e}")
        return pd.DataFrame()


def obtener_ct_detalle(url_base):
    """
    Pestaña Detalle (gid=150387248): A=barcode, B=laboratorio, C=cadena_o_cliente, D=descuento(%)
    Prioridad sobre Hoja1 cuando hay match barcode+lab+(cadena o cliente).
    Descarta filas con descuento no numérico.
    """
    try:
        df = pd.read_csv(url_con_gid(url_base, GID_DETALLE))
        df = df.rename(columns={
            df.columns[0]: 'barcode_det',
            df.columns[1]: 'laboratorio_det',
            df.columns[2]: 'cadena_cliente_det',
            df.columns[3]: 'descuento_det_raw',
        })

        desc_limpio   = df['descuento_det_raw'].astype(str).str.replace('%','',regex=False).str.strip()
        filas_validas = pd.to_numeric(desc_limpio, errors='coerce').notna()
        descartadas   = (~filas_validas).sum()
        if descartadas > 0:
            st.caption(f"⚠️ Detalle CT: {descartadas} fila(s) con descuento no numérico ignoradas.")
        df = df[filas_validas].copy()

        df['descuento_det']          = desc_limpio[filas_validas].astype(float) / 100
        df['barcode_det']            = estandarizar_barcodes(df['barcode_det'])
        df['laboratorio_det_key']    = df['laboratorio_det'].astype(str).str.strip().str.lower()
        df['cadena_cliente_det_key'] = df['cadena_cliente_det'].astype(str).str.strip().str.lower().apply(quitar_tildes)

        return df[['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key', 'descuento_det']]
    except Exception as e:
        st.warning(f"⚠️ No se pudo leer pestaña Detalle CT: {e}")
        return pd.DataFrame()


def obtener_excluidos_ct(url_base):
    """
    Pestaña Excluir (gid=1591504897): A=barcode, B=laboratorio
    Productos sin descuento CT para ese lab.
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


def obtener_sellout_desde_secrets():
    url = st.secrets.get("sellout_sheets", {}).get("url", "")
    if not url:
        return pd.DataFrame(), "No se encontró 'sellout_sheets.url' en secrets."
    df = obtener_ofertas_sheets(url)
    return (df, None) if not df.empty else (pd.DataFrame(), "Sheets SellOut vacío.")


# ─────────────────────────────────────────────
# APLICAR DESCUENTOS CT (lógica compartida)
# ─────────────────────────────────────────────

def aplicar_descuentos_ct(df_final, url_ct, cadena_filtro=None):
    """
    Aplica la lógica CT completa sobre df_final.

    Fuentes de descuento (independientes, se unen como append):
      - Hoja1: descuento general por cliente+lab (o cadena+lab para Farmago/Farmatención)
      - Detalle: descuento por barcode+lab+cadena_o_cliente (tiene prioridad si hay solapamiento)

    Flujo:
      1. Lee Hoja1, Detalle y Excluir del Sheets CT
      2. Filtra cada fuente por cadena si aplica
      3. Genera df_hoja1_result: merge Odoo × Hoja1
      4. Genera df_detalle_result: merge Odoo × Detalle
      5. Une ambos; si una línea aparece en los dos, Detalle gana
      6. Aplica lista Excluir (descuento_valor = 0 no: excluye la línea)
      7. Devuelve df_final enriquecido con 'descuento_valor' y debug_info
    """
    df_hoja1   = obtener_ct_hoja1(url_ct)
    df_detalle = obtener_ct_detalle(url_ct)
    df_excluir = obtener_excluidos_ct(url_ct)

    if df_hoja1.empty and df_detalle.empty:
        st.error("No se pudo leer ninguna fuente de descuentos CT.")
        return df_final, {}

    # ── Preparar claves sobre df_final ──────────────────────────────
    df_final = df_final.copy()
    df_final['partner_key']  = df_final['partner_id'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower()
    )
    df_final['lab_key'] = df_final['laboratory_name'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower()
    )
    df_final['cadena_key_f'] = df_final['cadena_val'].apply(
        lambda x: quitar_tildes((x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower())
        if x else ''
    )
    df_final['barcode_norm'] = estandarizar_barcodes(
        df_final['barcode'].apply(lambda x: x if isinstance(x, str) else str(x))
    )

    # ── PARTE A: resultado desde Hoja1 ──────────────────────────────
    df_hoja1_result = pd.DataFrame()
    if not df_hoja1.empty:
        cadena_norm = quitar_tildes(cadena_filtro.lower().strip()) if cadena_filtro else None

        if cadena_norm:
            df_h1_filt = df_hoja1[df_hoja1['cadena_key'] == cadena_norm].copy()
        else:
            df_h1_filt = df_hoja1[~df_hoja1['cadena_key'].isin(CADENAS_FARMAGO_FARMATENCION)].copy()

        if not df_h1_filt.empty:
            if cadena_norm:
                df_h1_merge = df_h1_filt.drop_duplicates(subset=['cadena_key', 'laboratorio_key'])
                df_hoja1_result = df_final.merge(
                    df_h1_merge[['cadena_key', 'laboratorio_key', 'descuento_ct']],
                    left_on=['cadena_key_f', 'lab_key'],
                    right_on=['cadena_key', 'laboratorio_key'],
                    how='inner'
                )
            else:
                df_hoja1_result = df_final.merge(
                    df_h1_filt[['partner_name_key', 'laboratorio_key', 'descuento_ct']],
                    left_on=['partner_key', 'lab_key'],
                    right_on=['partner_name_key', 'laboratorio_key'],
                    how='inner'
                )
            df_hoja1_result = df_hoja1_result.rename(columns={'descuento_ct': 'descuento_valor'})
            df_hoja1_result['_fuente'] = 'hoja1'
    else:
        df_h1_filt = pd.DataFrame()

    # ── PARTE B: resultado desde Detalle ────────────────────────────
    df_detalle_result = pd.DataFrame()
    if not df_detalle.empty:
        cadena_norm = quitar_tildes(cadena_filtro.lower().strip()) if cadena_filtro else None

        # Filtrar Detalle por cadena si aplica
        if cadena_norm:
            df_det_filt = df_detalle[
                df_detalle['cadena_cliente_det_key'] == cadena_norm
            ].copy()
        else:
            df_det_filt = df_detalle[
                ~df_detalle['cadena_cliente_det_key'].isin(CADENAS_FARMAGO_FARMATENCION)
            ].copy()

        if not df_det_filt.empty:
            # ── DEBUG DETALLE (comentado) ─────────────────────────────
            #             st.write("🔍 DEBUG Detalle — filas en df_det_filt:", len(df_det_filt))
            #             st.dataframe(df_det_filt.head(10))
            #             st.write("🔍 DEBUG Odoo — claves barcode_norm × lab_key (primeras 20):")
            #             st.dataframe(
            #                 df_final[['barcode_norm', 'lab_key', 'cadena_key_f', 'partner_key']]
            #                 .drop_duplicates().head(20)
            #             )
            #             st.write("🔍 DEBUG Detalle — claves barcode_det × laboratorio_det_key × cadena_cliente_det_key:")
            #             st.dataframe(df_det_filt[['barcode_det','laboratorio_det_key','cadena_cliente_det_key','descuento_det']].head(20))
            #             # Match por cadena O por cliente — tomamos el que matchee
            merge_cad = df_final.merge(
                df_det_filt[['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key', 'descuento_det']],
                left_on=['barcode_norm', 'lab_key', 'cadena_key_f'],
                right_on=['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key'],
                how='inner'
            )
            merge_cli = df_final.merge(
                df_det_filt[['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key', 'descuento_det']],
                left_on=['barcode_norm', 'lab_key', 'partner_key'],
                right_on=['barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key'],
                how='inner'
            )
            # Unir ambos matches y deduplicar (cadena tiene prioridad)
            df_detalle_result = pd.concat([merge_cad, merge_cli], ignore_index=True)
            # Deduplicar por línea de factura: si aparece por cadena Y por cliente, cadena gana
            id_cols = ['id', 'move_id_int', 'product_id_int', 'barcode_norm', 'lab_key']
            id_cols_present = [c for c in id_cols if c in df_detalle_result.columns]
            if id_cols_present:
                df_detalle_result = df_detalle_result.drop_duplicates(
                    subset=id_cols_present, keep='first'
                )
            df_detalle_result = df_detalle_result.rename(columns={'descuento_det': 'descuento_valor'})
            df_detalle_result['_fuente'] = 'detalle'

    # ── PARTE C: unir Hoja1 + Detalle ───────────────────────────────
    # Si una línea aparece en ambos, Detalle gana
    if df_hoja1_result.empty and df_detalle_result.empty:
        st.caption("⚠️ Sin matches en Hoja1 ni Detalle CT.")
        df_final_out = pd.DataFrame()
        debug_info = {'hoja1': pd.DataFrame(), 'odoo_keys': pd.DataFrame(), 'sin_match': pd.DataFrame()}
        return df_final_out, debug_info

    partes = [p for p in [df_hoja1_result, df_detalle_result] if not p.empty]
    df_union = pd.concat(partes, ignore_index=True)

    # Deduplicar: por línea de factura, Detalle (último en concat si ordenamos) gana
    # Ordenar: detalle al final para que keep='last' lo preserve
    df_union = df_union.sort_values('_fuente', ascending=True)  # hoja1 < detalle alfabéticamente
    id_dedup = [c for c in ['id', 'move_id_int', 'product_id_int'] if c in df_union.columns]
    if id_dedup:
        df_union = df_union.drop_duplicates(subset=id_dedup, keep='last')

    n_hoja1   = (df_union['_fuente'] == 'hoja1').sum()
    n_detalle = (df_union['_fuente'] == 'detalle').sum()
    st.toast(f"CT: {n_hoja1} desde Hoja1 · {n_detalle} desde Detalle")

    df_union = df_union.drop(columns=['_fuente'], errors='ignore')

    # ── PARTE D: aplicar lista Excluir ──────────────────────────────
    if not df_excluir.empty and 'barcode_norm' in df_union.columns and 'lab_key' in df_union.columns:
        excl_check = df_union[['barcode_norm', 'lab_key']].merge(
            df_excluir,
            left_on=['barcode_norm', 'lab_key'],
            right_on=['barcode_excluido', 'laboratorio_excluir_key'],
            how='left', indicator=True
        )
        mask_excluir = (excl_check['_merge'] == 'both').values
        n_excl = mask_excluir.sum()
        if n_excl > 0:
            st.caption(f"ℹ️ {n_excl} línea(s) excluidas por lista sin descuento CT.")
        df_union = df_union[~mask_excluir].copy()

    # ── Debug info ───────────────────────────────────────────────────
    debug_odoo_keys = (
        df_union[['partner_key', 'lab_key']].drop_duplicates()
        .sort_values(['partner_key', 'lab_key']).reset_index(drop=True)
        if 'partner_key' in df_union.columns else pd.DataFrame()
    )
    sin_match = pd.DataFrame()
    if not df_h1_filt.empty:
        merged_check = df_h1_filt.merge(
            debug_odoo_keys,
            left_on=['partner_name_key', 'laboratorio_key'],
            right_on=['partner_key', 'lab_key'],
            how='left', indicator=True
        ) if 'partner_name_key' in df_h1_filt.columns else pd.DataFrame()
        if not merged_check.empty:
            sin_match = merged_check[merged_check['_merge'] == 'left_only'][
                [c for c in ['partner_name', 'cadena', 'laboratorio', 'partner_name_key', 'laboratorio_key']
                 if c in merged_check.columns]
            ].reset_index(drop=True)

    debug_info = {
        'hoja1':     df_hoja1[['partner_name', 'partner_name_key', 'cadena', 'cadena_key',
                                'laboratorio', 'laboratorio_key', 'descuento_ct']].reset_index(drop=True)
                    if not df_hoja1.empty else pd.DataFrame(),
        'odoo_keys': debug_odoo_keys,
        'sin_match': sin_match,
        'n_detalle': n_detalle if not df_detalle_result.empty else 0,
    }

    # Limpiar columnas auxiliares
    df_union = df_union.drop(
        columns=['partner_key', 'lab_key', 'cadena_key_f', 'barcode_norm',
                 'partner_name_key', 'laboratorio_key',
                 'barcode_det', 'laboratorio_det_key', 'cadena_cliente_det_key',
                 'cadena_key'],
        errors='ignore'
    )
    return df_union, debug_info


def aplicar_comparacion_sellout_vs_ct(df_final, df_ct_ref):
    """
    SELL-OUT: excluir líneas donde CT > SellOut (las reportará CT).
    df_final debe tener 'descuento_valor' normalizado.
    """
    if df_ct_ref.empty:
        return df_final

    df_final['partner_key_tmp'] = df_final['partner_id'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower()
    )
    df_final['lab_key_tmp'] = df_final['laboratory_name'].apply(
        lambda x: (x[1] if isinstance(x, (list, tuple)) else str(x)).strip().lower()
    )

    df_final = df_final.merge(
        df_ct_ref[['partner_name_key', 'laboratorio_key', 'descuento_ct']],
        left_on=['partner_key_tmp', 'lab_key_tmp'],
        right_on=['partner_name_key', 'laboratorio_key'],
        how='left'
    )

    tiene_ct = df_final['descuento_ct'].notna()
    ct_gana  = df_final['descuento_ct'] > df_final['descuento_valor']
    n_excl   = (tiene_ct & ct_gana).sum()
    if n_excl > 0:
        st.caption(f"ℹ️ SELL-OUT: {n_excl} línea(s) con mayor descuento CT — irán en CT Fijo.")

    df_final = df_final[~(tiene_ct & ct_gana)].copy()
    df_final = df_final.drop(
        columns=['partner_key_tmp', 'lab_key_tmp', 'partner_name_key', 'laboratorio_key', 'descuento_ct'],
        errors='ignore'
    )
    return df_final


def aplicar_comparacion_ct_vs_sellout(df_final, df_so_ref, fecha_inicio, fecha_fin):
    """
    CT Fijo / Farmago / Farmatención:
    Si SellOut > CT para ese barcode → descuento_valor = 0, gano_sellout = True.
    Esas líneas van en Excel pero NO se envían a Sheets.
    """
    df_final['gano_sellout'] = False
    if df_so_ref.empty:
        return df_final

    vigentes = df_so_ref[
        (df_so_ref['oferta_inicio'] <= fecha_fin) &
        (df_so_ref['oferta_fin']    >= fecha_inicio)
    ][['barcode_key', 'descuento_so']].copy()

    if vigentes.empty:
        return df_final

    df_final['barcode_tmp'] = estandarizar_barcodes(
        df_final['barcode'].apply(lambda x: x if isinstance(x, str) else str(x))
    )
    df_final = df_final.merge(
        vigentes.rename(columns={'descuento_so': 'descuento_so_tmp'}),
        left_on='barcode_tmp', right_on='barcode_key',
        how='left'
    )

    tiene_so = df_final['descuento_so_tmp'].notna()
    so_gana  = df_final['descuento_so_tmp'] > df_final['descuento_valor']
    df_final['gano_sellout'] = tiene_so & so_gana
    df_final.loc[df_final['gano_sellout'], 'descuento_valor'] = 0

    n_cero = df_final['gano_sellout'].sum()
    if n_cero > 0:
        st.caption(f"ℹ️ {n_cero} línea(s) con descuento 0 — SellOut tiene mayor porcentaje.")

    df_final = df_final.drop(columns=['barcode_tmp', 'barcode_key', 'descuento_so_tmp'], errors='ignore')
    return df_final


# ─────────────────────────────────────────────
# MOTOR DE EXCEL
# ─────────────────────────────────────────────

def motor_split_laboratorios(df_final, config_costos=None):
    if df_final.empty:
        return {}

    config_costos = config_costos or {}
    diccionario_excels = {}
    CADENAS_PRECIO_FULL = ['farmago', 'farmatención']
    tipo_activo = st.session_state.get('tipo_reporte_activo', '')

    for lab in df_final['laboratory_name'].unique():
        df_lab = df_final[
        (df_final['laboratory_name'] == lab) &
        (df_final['gano_sellout'] == False)
        ].copy()
        es_a_costo = config_costos.get(lab, False)

        if df_lab.empty:          # ← AGREGAR ESTO
            continue     

        for col in ['quantity', 'price_unit', 'costo_laboratorio', 'descuento_valor']:
            if col in df_lab.columns:
                df_lab[col] = df_lab[col].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else x)
                df_lab[col] = pd.to_numeric(df_lab[col], errors='coerce').fillna(0)

        if es_a_costo:
            df_lab['valor_calculado'] = df_lab['costo_laboratorio']
        elif tipo_activo == 'SELL-OUT':
            def calcular_precio_fila(row):
                cadena = str(row.get('cadena', '')).lower().strip()
                if any(c in cadena for c in CADENAS_PRECIO_FULL):
                    return row['price_unit']
                d = row['descuento_valor']
                if d >= 1 or d < 0:
                    return row['price_unit']
                return row['price_unit'] / (1 - d)
            df_lab['valor_calculado'] = df_lab.apply(calcular_precio_fila, axis=1)
        else:
            df_lab['valor_calculado'] = df_lab['price_unit']

        reporte = pd.DataFrame({
            'invoice_date':        df_lab['invoice_date'],
            'partner_id_num':      df_lab['partner_id_num'],
            'partner_id':          df_lab['partner_id'],
            'invoice_number_next': df_lab['invoice_number_next'],
            'barcode':             df_lab['barcode'],
            'name':                df_lab['name'],
            'laboratory_name':     df_lab['laboratory_name'],
            'supplier_code':       df_lab['supplier_code'],
            'quantity':            df_lab['quantity'],
            'valor_unitario':      df_lab['valor_calculado'],
            'descuento':           df_lab['descuento_valor'],
            'Moneda':              df_lab['currency_id'],
        })
        reporte['subtotal_bruto']  = reporte['quantity'] * reporte['valor_unitario']
        reporte['total_descuento'] = reporte['subtotal_bruto'] * reporte['descuento']

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            reporte_export = reporte.drop(columns=['Moneda'])
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
        diccionario_excels[lab] = output.getvalue()

    return diccionario_excels


# ─────────────────────────────────────────────
# ENVÍO A SHEETS
# ─────────────────────────────────────────────

def enviar_a_sheets(df_display, fecha_inicio, fecha_fin, apps_script_url, config_costos=None):
    config_costos = config_costos or {}
    CADENAS_PRECIO_FULL = ['farmago', 'farmatención']
    tipo_activo = st.session_state.get('tipo_reporte_activo', '')

    if tipo_activo == 'SELL-OUT':
        concepto = f"Sell-Out del {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')} (en Panel)"
    else:
        concepto = f"{tipo_activo} del {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}"
    meses_es = {
        'January':'Enero','February':'Febrero','March':'Marzo','April':'Abril',
        'May':'Mayo','June':'Junio','July':'Julio','August':'Agosto',
        'September':'Septiembre','October':'Octubre','November':'Noviembre','December':'Diciembre'
    }
    mes = f"{meses_es.get(fecha_inicio.strftime('%B'), fecha_inicio.strftime('%B'))} {fecha_inicio.strftime('%Y')}"

    df = df_display.copy()

    # Excluir líneas donde SellOut ganó en reportes CT
    if tipo_activo in ('Descuentos CT Lineal', 'Farmago', 'Farmatención') and 'gano_sellout' in df.columns:
        df = df[~df['gano_sellout']].copy()

    for col in ['quantity', 'price_unit', 'costo_laboratorio', 'descuento_valor']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    resultados, errores = [], []
    for lab in df['laboratory_name'].unique():
        df_lab = df[df['laboratory_name'] == lab].copy()
        es_a_costo = config_costos.get(lab, False)

        if es_a_costo:
            df_lab['valor_calculado'] = df_lab['costo_laboratorio']
        elif tipo_activo == 'SELL-OUT':
            def calcular_precio_fila(row):
                cadena = str(row['cadena']).lower().strip()
                if any(c in cadena for c in CADENAS_PRECIO_FULL):
                    return row['price_unit']
                d = row['descuento_valor']
                if d >= 1 or d < 0:
                    return row['price_unit']
                return row['price_unit'] / (1 - d)
            df_lab['valor_calculado'] = df_lab.apply(calcular_precio_fila, axis=1)
        else:
            df_lab['valor_calculado'] = df_lab['price_unit']

        df_lab['subtotal_bruto']  = df_lab['quantity'] * df_lab['valor_calculado']
        df_lab['total_descuento'] = df_lab['subtotal_bruto'] * df_lab['descuento_valor']

        moneda = str(df_lab['currency_id'].iloc[0]).lower().strip()
        total  = df_lab['total_descuento'].sum()
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
                resultados.append(lab)
            else:
                errores.append(f"{lab}: {result.get('error', 'error desconocido')}")
        except Exception as ex:
            errores.append(f"{lab}: {str(ex)}")

    return resultados, errores


# ─────────────────────────────────────────────
# CORREOS
# ─────────────────────────────────────────────

def generar_mailto(lab, fecha_inicio, fecha_fin, comment_por_lab, cc_emails):
    correos_to = extraer_correos_html(comment_por_lab.get(lab, ''))
    tipo_activo = st.session_state.tipo_reporte_activo

    asunto = f"{lab} | Reporte {tipo_activo} del {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}"
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
    st.header("🎯 Panel de Reportes")

    def limpiar(val):
        return val[1] if isinstance(val, (list, tuple)) else val

    def limpiar_barcode(val):
        if not val: return ""
        return str(val).split('.')[0].strip()

    for key, default in [
        ('df_resultado', None), ('archivos_binarios', {}),
        ('tipo_reporte_activo', ''), ('config_costos', {}),
        ('config_costos_aplicada', {}), ('comment_por_lab', {}),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    if st.session_state.get('_reset_checkboxes', False):
        for k in [k for k in st.session_state if k.startswith("chk_")]:
            del st.session_state[k]
        st.session_state._reset_checkboxes = False

    # ── 1. SELECTOR ─────────────────────────────────────────────────
    tipo_reporte = st.radio(
        "Seleccione el tipo de análisis:",
        ["SELL-OUT", "Descuentos CT Lineal", "Farmago", "Farmatención", "Extracción General"],
        horizontal=True, key="selector_principal"
    )
    st.divider()

    # ── 2. INPUTS DE REFERENCIA ──────────────────────────────────────
    url_so = url_ct = ""
    df_referencia = pd.DataFrame()

    if tipo_reporte == "SELL-OUT":
        url_so = st.text_input(
            "Link de Google Sheets (SellOut)",
            "https://docs.google.com/spreadsheets/d/1c4Eil9IoOhUTNr3_jrZn5HI5GNZq9NTkgPH0CbjwYMA/export?format=csv"
        )
        if url_so and url_so.startswith("https://"):
            df_referencia = obtener_ofertas_sheets(url_so)
            if not df_referencia.empty:
                st.caption("✅ Ofertas SellOut cargadas.")

    elif tipo_reporte in ("Descuentos CT Lineal", "Farmago", "Farmatención"):
        url_ct = st.text_input(
            "Link de Google Sheets (CT Fijo)",
            "https://docs.google.com/spreadsheets/d/1R6xw2K5sHyRIMDAlr3fn0xNJewy8mRsiYf58TH1A-EA/export?format=csv"
        )
        if url_ct and url_ct.startswith("https://"):
            # Solo mostrar conteo de reglas relevantes
            df_h1 = obtener_ct_hoja1(url_ct)
            if not df_h1.empty:
                if tipo_reporte == "Farmago":
                    n = df_h1[df_h1['cadena_key'] == 'farmago'].shape[0]
                elif tipo_reporte == "Farmatención":
                    n = df_h1[df_h1['cadena_key'] == 'farmatencion'].shape[0]
                else:
                    n = df_h1[~df_h1['cadena_key'].isin(CADENAS_FARMAGO_FARMATENCION)].shape[0]
                st.caption(f"✅ {n} reglas CT para {tipo_reporte}.")
            df_referencia = df_h1  # solo para validar que no está vacío

    # ── 3. EJECUCIÓN ─────────────────────────────────────────────────
    if st.button("🚀 Generar Reporte", type="primary"):
        if tipo_reporte in ("Descuentos CT Lineal", "Farmago", "Farmatención") and df_referencia.empty:
            st.warning("Por favor ingresa el link del Sheets CT antes de generar el reporte.")
            return

        try:
            for k in [k for k in st.session_state if k.startswith("chk_")]:
                st.session_state[k] = False
            st.session_state.config_costos          = {}
            st.session_state.config_costos_aplicada = {}

            config = st.secrets["odoo_bd1"]
            client = OdooClient(config["url"], config["db"], config["username"], config["password"])

            domain = [
                ('date', '>=', str(fecha_inicio)), ('date', '<=', str(fecha_fin)),
                ('move_type', '=', 'out_invoice'),  ('parent_state', '=', 'posted'),
                ('move_name', 'not ilike', 'ND%'),  ('product_id', '!=', False),
                ('quantity', '>', 0),
            ]

            # Filtro de domain por tipo
            if tipo_reporte == "SELL-OUT" and not df_referencia.empty:
                domain.append(('product_id.barcode', 'in',
                                df_referencia['barcode_key'].unique().tolist()))
            elif tipo_reporte == "Farmago":
                domain.append(('partner_id.cadena', 'ilike', 'Farmago'))
            elif tipo_reporte == "Farmatención":
                domain.append(('partner_id.cadena', 'ilike', 'Farmatención'))

            with st.spinner("Consultando Odoo..."):
                data_lineas = client.search_read(
                    'account.move.line', domain,
                    ['move_id', 'product_id', 'name', 'quantity', 'price_unit']
                )
                if not data_lineas:
                    st.warning("No hay datos para esta selección.")
                    return
                df_lineas = pd.DataFrame(data_lineas)

                move_ids    = list(set([x[0] for x in df_lineas['move_id']    if isinstance(x, list)]))
                product_ids = list(set([x[0] for x in df_lineas['product_id'] if isinstance(x, list)]))

                df_moves = pd.DataFrame(client.search_read(
                    'account.move', [('id', 'in', move_ids)],
                    ['invoice_date', 'partner_id', 'invoice_number_next', 'currency_id']
                )).rename(columns={'id': 'move_id_int'})

                df_prods = pd.DataFrame(client.search_read(
                    'product.product', [('id', 'in', product_ids)],
                    ['laboratory_name', 'supplier_code', 'barcode']
                )).rename(columns={'id': 'product_id_int'})

                data_costs = client.search_read(
                    'product.supplierinfo', [('product_tmpl_id', 'in', product_ids)],
                    ['product_tmpl_id', 'price']
                )
                if data_costs:
                    df_costs = pd.DataFrame(data_costs)
                    df_costs['product_id_int'] = df_costs['product_tmpl_id'].apply(
                        lambda x: x[0] if isinstance(x, (list, tuple)) else x)
                    df_costs = df_costs.rename(columns={'price': 'costo_proveedor'}).drop_duplicates('product_id_int')
                else:
                    df_costs = pd.DataFrame(columns=['product_id_int', 'costo_proveedor'])

                partner_ids_raw = list(set([
                    m['partner_id'][0] for m in df_moves.to_dict('records')
                    if isinstance(m.get('partner_id'), (list, tuple))
                ]))
                df_partners = pd.DataFrame(client.search_read(
                    'res.partner', [('id', 'in', partner_ids_raw)], ['id', 'cadena']
                )).rename(columns={'id': 'partner_id_int', 'cadena': 'cadena_val'})

                df_lineas['move_id_int']    = df_lineas['move_id'].apply(lambda x: x[0] if isinstance(x, list) else x)
                df_lineas['product_id_int'] = df_lineas['product_id'].apply(lambda x: x[0] if isinstance(x, list) else x)

                df_final = df_lineas.merge(df_moves, on='move_id_int', how='left')
                df_final = df_final.merge(df_prods,  on='product_id_int', how='left')
                df_final = df_final.merge(df_costs[['product_id_int', 'costo_proveedor']], on='product_id_int', how='left')
                df_final['partner_id_int'] = df_final['partner_id'].apply(
                    lambda x: x[0] if isinstance(x, (list, tuple)) else None)
                df_final = df_final.merge(df_partners, on='partner_id_int', how='left')

                # Correos por laboratorio
                lab_names = list({
                    (x[1] if isinstance(x, (list, tuple)) else x)
                    for x in df_final['laboratory_name'].dropna() if x
                })
                df_lab_partners = pd.DataFrame(client.search_read(
                    'res.partner', [('name', 'in', lab_names)], ['id', 'name', 'comment']
                ))
                comment_por_lab = {}
                for _, row in df_lab_partners.iterrows():
                    c = row['comment']
                    comment_por_lab[row['name']] = str(c).strip() if c and c is not False else ''
                st.session_state.comment_por_lab = comment_por_lab

                # ── LÓGICA POR TIPO ──────────────────────────────────────────
                debug_info   = {}
                gano_sellout = False

                if tipo_reporte == "SELL-OUT" and not df_referencia.empty:
                    df_final['barcode_key_tmp'] = estandarizar_barcodes(df_final['barcode'])
                    df_final = df_final.merge(
                        df_referencia, left_on='barcode_key_tmp', right_on='barcode_key', how='inner'
                    )
                    df_final['invoice_date_obj'] = pd.to_datetime(df_final['invoice_date']).dt.date
                    df_final = df_final[
                        (df_final['invoice_date_obj'] >= df_final['oferta_inicio']) &
                        (df_final['invoice_date_obj'] <= df_final['oferta_fin'])
                    ]
                    df_final['descuento_valor'] = df_final['descuento_so'].copy()

                    # Comparar vs CT Hoja1
                    df_ct_h1 = obtener_ct_hoja1(st.secrets.get("ct_fijo_sheets", {}).get("url", ""))
                    if not df_ct_h1.empty:
                        df_final = aplicar_comparacion_sellout_vs_ct(df_final, df_ct_h1)
                    df_final['gano_sellout'] = False

                elif tipo_reporte in ("Descuentos CT Lineal", "Farmago", "Farmatención"):
                    cadena_filtro = tipo_reporte if tipo_reporte in ("Farmago", "Farmatención") else None
                    df_final, debug_info = aplicar_descuentos_ct(df_final, url_ct, cadena_filtro)

                    if df_final.empty:
                        st.caption("⚠️ Sin matches entre Odoo y Sheets CT para el período seleccionado.")
                        return

                    # Comparar vs SellOut
                    df_so_ref, err_so = obtener_sellout_desde_secrets()
                    if err_so:
                        st.caption(f"⚠️ Comparación SellOut omitida: {err_so}")
                        df_final['gano_sellout'] = False
                    else:
                        df_final = aplicar_comparacion_ct_vs_sellout(
                            df_final, df_so_ref, fecha_inicio, fecha_fin
                        )

                if 'descuento_valor' not in df_final.columns:
                    df_final['descuento_valor'] = 0
                if 'gano_sellout' not in df_final.columns:
                    df_final['gano_sellout'] = False

                st.session_state.debug_info = debug_info

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
                    'gano_sellout':        df_final['gano_sellout'],
                })

                st.session_state.df_resultado        = res
                st.session_state.tipo_reporte_activo = tipo_reporte
                st.session_state.archivos_binarios   = motor_split_laboratorios(res, st.session_state.config_costos)
                st.rerun()

        except Exception as e:
            st.error(f"Error crítico: {e}")

    # ── 4. RENDERIZADO ───────────────────────────────────────────────
    if st.session_state.df_resultado is not None:
        df_display  = st.session_state.df_resultado
        tipo_activo = st.session_state.tipo_reporte_activo
        labs_encontrados = sorted(df_display['laboratory_name'].unique())

        with st.sidebar:
            st.header("⚙️ Configuración de Reporte")
            st.info("Seleccione los laboratorios que desea exportar a **COSTO**.")
            for lab in labs_encontrados:
                st.session_state.config_costos[lab] = st.checkbox(f"{lab}", key=f"chk_{lab}")
            st.divider()
            if st.button("🔄 Aplicar y Regenerar Excels", use_container_width=True, type="primary"):
                st.session_state.archivos_binarios      = motor_split_laboratorios(df_display, st.session_state.config_costos)
                st.session_state.config_costos_aplicada = st.session_state.config_costos.copy()
                st.toast("Archivos de Excel actualizados con éxito")
                st.rerun()

        st.success(f"✅ Extracción finalizada ({tipo_activo}): {len(df_display)} registros.")

        # Debug CT
        # if tipo_activo in ('Descuentos CT Lineal', 'Farmago', 'Farmatención'):
        #     dbg = st.session_state.get('debug_info', {})
        #     if dbg:
        #         with st.expander(f"🔍 Debug CT — {tipo_activo}", expanded=False):
        #             st.write("**Reglas Hoja1 usadas:**")
        #             st.dataframe(dbg.get('hoja1', pd.DataFrame()))
        #             st.write("**Combinaciones cliente+lab en Odoo:**")
        #             st.dataframe(dbg.get('odoo_keys', pd.DataFrame()))
        #             sin_match = dbg.get('sin_match', pd.DataFrame())
        #             if sin_match.empty:
        #                 st.success("✅ Todas las reglas tienen match en Odoo")
        #             else:
        #                 st.warning(f"⚠️ {len(sin_match)} regla(s) SIN match en Odoo:")
        #                 st.dataframe(sin_match)

        #     n_so = df_display['gano_sellout'].sum() if 'gano_sellout' in df_display.columns else 0
        #     if n_so > 0:
        #         st.caption(f"⚠️ {n_so} línea(s) con descuento 0 en CT (SellOut mayor). Aparecen en Excel, no se envían a Sheets.")

        col1, _ = st.columns([1, 4])
        with col1:
            if st.button("🗑️ Limpiar Todo"):
                st.session_state.df_resultado      = None
                st.session_state.archivos_binarios = {}
                st.rerun()

        cols_display = [c for c in df_display.columns if c != 'gano_sellout']
        st.dataframe(df_display[cols_display], use_container_width=True)

        st.divider()
        st.subheader("📤 Enviar resumen a Google Sheets")
        apps_url = st.text_input(
            "URL del Apps Script",
            value=st.secrets["appscript"]["url"],
            key="input_apps_script_url"
        )
        if st.button("📨 Enviar resumen NC a Sheets", type="primary"):
            if not apps_url:
                st.error("Ingresa la URL del Apps Script.")
            else:
                with st.spinner("Enviando datos..."):
                    ok, errores = enviar_a_sheets(
                        df_display, fecha_inicio, fecha_fin, apps_url,
                        config_costos=st.session_state.config_costos
                    )
                if ok:
                    st.success(f"✅ Enviados: {', '.join(ok)}")
                for e in errores:
                    st.error(f"❌ {e}")

        # Descargas
        if st.session_state.archivos_binarios:
            st.write("### 📥 Descargar por Laboratorio")

            # Labs con Excel generado
            labs_con_excel = set(st.session_state.archivos_binarios.keys())

            # Labs sin Excel (gano_sellout en todas sus filas)
            labs_sin_excel = [
                lab for lab in labs_encontrados
                if lab not in labs_con_excel
            ]

            if labs_sin_excel:
                st.warning(
                    f"⚠️ Los siguientes laboratorios no tienen líneas para exportar "
                    f"(SellOut superó CT en todas sus líneas): "
                    f"**{', '.join(labs_sin_excel)}**"
                )

            items = list(st.session_state.archivos_binarios.items())
            for i in range(0, len(items), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(items):
                        lab, excel_data = items[i + j]
                        es_costo = st.session_state.config_costos_aplicada.get(lab, False)
                        etiqueta = " (Costo)" if es_costo else ""
                        safe_lab = (
                            lab.replace(" ","_").replace("/","").replace("\\","").replace(":","")
                            .replace("á","a").replace("é","e").replace("í","i")
                            .replace("ó","o").replace("ú","u").replace("Á","A")
                            .replace("É","E").replace("Í","I").replace("Ó","O")
                            .replace("Ú","U").replace("ü","u").replace("Ü","U")
                            .replace("ñ","n").replace("Ñ","N")
                        )
                        with cols[j]:
                            st.download_button(
                                label=f"📦 {lab}{etiqueta}",
                                data=excel_data,
                                file_name=(
                                    f"{safe_lab}_{tipo_activo}_del_"
                                    f"{fecha_inicio.strftime('%d-%m-%Y')}_al_"
                                    f"{fecha_fin.strftime('%d-%m-%Y')}.xlsx"
                                ),
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dl_{lab}_{tipo_activo}_{i+j}".replace(" ","_")
                            )
        # Correos
        if tipo_activo in ('SELL-OUT', 'Descuentos CT Lineal', 'Farmago', 'Farmatención'):
            st.divider()
            st.subheader("📧 Enviar correos a laboratorios")
            CC_DEFAULT = ["staddeo.blv@gmail.com", "staddeo@drogueriablv.com"]
            cc_input  = st.text_input("CC adicionales (separados por coma)",
                                       value=", ".join(CC_DEFAULT), key="cc_emails_input")
            CC_EMAILS = [e.strip() for e in cc_input.split(",") if e.strip()]
            comment_por_lab = st.session_state.get('comment_por_lab', {})
            labs_con_excel = set(st.session_state.archivos_binarios.keys())
            for i in range(0, len(labs_encontrados), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(labs_encontrados):
                        lab = labs_encontrados[i + j]
                        mailto, correos_to = generar_mailto(
                            lab, fecha_inicio, fecha_fin, comment_por_lab, CC_EMAILS
                        )
                        tiene_correos = len(correos_to) > 0
                        sin_excel     = lab not in labs_con_excel   # ← AGREGAR

                        if sin_excel:
                            etiqueta = f"🚫 {lab} (sin líneas CT)"  # ← AGREGAR
                        elif tiene_correos:
                            etiqueta = f"✉️ {lab}"
                        else:
                            etiqueta = f"⚠️ {lab} (sin correos)"

                        with cols[j]:
                            st.link_button(
                                etiqueta,
                                mailto,
                                disabled=not tiene_correos or sin_excel   # ← MODIFICAR
                            )
