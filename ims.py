#modulo_master_data
# ─────────────────────────────────────────────────────────────────
# Genera UN ÚNICO Excel "Master Data" con 3 pestañas, a partir del mismo
# rango de fechas (fecha_inicio, fecha_fin) que usa modulo_consolidado:
#
#   1) MAEINV  → catálogo completo de productos (product.product)
#   2) MAECLI  → contactos (res.partner) cuyas etiquetas (category_id)
#                NO incluyen "Laboratorio"
#   3) FACMES  → líneas de facturas (out_invoice) y notas de crédito
#                (out_refund) cuyo campo refund_cause NO esté vacío,
#                emitidas dentro del rango de fechas seleccionado
#
# Reutiliza exactamente el mismo patrón de conexión/reintentos a Odoo y de
# manejo de secrets.toml que modulo_consolidado (get_odoo_client /
# odoo_search_read). Si ambos módulos conviven en la misma app, lo ideal es
# mover esas funciones a un archivo común (p. ej. odoo_utils.py) e
# importarlas desde ahí; acá se dejan replicadas para que este módulo sea
# autocontenido.
# ─────────────────────────────────────────────────────────────────

import streamlit as st
import pandas as pd
import numpy as np
import io
import time
import unicodedata
from datetime import date
from odoo_utils import OdooClient

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
REINTENTOS_ODOO      = 3
ESPERA_REINTENTO_SEG = 4

MESES_ABREV_ES = {
    1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Ago', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic',
}


# ─────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────

def quitar_tildes(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s))
                    if unicodedata.category(c) != 'Mn')

def limpiar_odoo(val):
    """Un campo many2one de Odoo llega como [id, 'Nombre']; nos quedamos con el nombre."""
    return val[1] if isinstance(val, (list, tuple)) else val

def id_odoo(val):
    """De un campo many2one de Odoo, nos quedamos con el id."""
    return val[0] if isinstance(val, (list, tuple)) else val

def es_moneda_usd(val):
    """Determina si un campo currency_id (many2one) de Odoo corresponde a USD."""
    nombre = quitar_tildes(str(limpiar_odoo(val))).strip().lower()
    return any(token in nombre for token in ['usd', 'dolar', '$'])


# ─────────────────────────────────────────────
# CONEXIÓN A ODOO — mismo patrón que modulo_consolidado
# ─────────────────────────────────────────────

def get_odoo_client():
    if '_odoo_client' not in st.session_state or st.session_state._odoo_client is None:
        config = st.secrets["odoo_bd1"]
        st.session_state._odoo_client = OdooClient(
            config["url"], config["db"], config["username"], config["password"]
        )
    return st.session_state._odoo_client


def odoo_search_read(model, domain, fields, intentos=REINTENTOS_ODOO, espera_seg=ESPERA_REINTENTO_SEG):
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
                st.session_state._odoo_client = None
                time.sleep(espera_seg)
    raise ultimo_error


# ─────────────────────────────────────────────
# NOMBRE DE ARCHIVO
# ─────────────────────────────────────────────

def construir_nombre_archivo(fecha_inicio, fecha_fin, fecha_extraccion=None):
    """
    Formato: "{MM}. Master Data DROGUERÍA BLV, C.A. - {Mes3} {DD extracción} – {dd-mm inicio} al {dd-mm fin}.xlsx"
    Ej: hoy 26/07/2026, rango 06/07 al 12/07 →
        "07. Master Data DROGUERÍA BLV, C.A. - Jul 26 – 06-07 al 12-07.xlsx"

    El número/nombre de mes que titula el archivo corresponde al mes en que
    se EXTRAE el reporte (hoy), no al mes del rango de fechas facturado.
    """
    if fecha_extraccion is None:
        fecha_extraccion = date.today()

    numero_mes = f"{fecha_extraccion.month:02d}"
    mes_3letras = MESES_ABREV_ES[fecha_extraccion.month]
    dia_extraccion = f"{fecha_extraccion.day:02d}"
    fi = fecha_inicio.strftime('%d-%m')
    ff = fecha_fin.strftime('%d-%m')

    return (
        f"{numero_mes}. Master Data DROGUERÍA BLV, C.A. - {mes_3letras} {dia_extraccion} "
        f"– {fi} al {ff}.xlsx"
    )


# ─────────────────────────────────────────────
# PESTAÑA 1: MAEINV — catálogo de productos
# ─────────────────────────────────────────────

def obtener_maeinv():
    """
    COD ART      → barcode (product.product)
    ART DES      → name (product.product)
    PROV DES     → laboratory_name (product.product)
    PRECIO VENTA → list_price_usd (product.template)

    Incluye una columna interna '_product_id' (no se exporta al Excel; la
    ignora _escribir_hoja_simple) que se usa luego para detectar si algún
    producto vendido en FACMES (posiblemente archivado) no está presente
    acá, y así poder agregarlo.
    """
    data_prod = odoo_search_read(
        'product.product',
        [('active', '=', True)],
        ['barcode', 'name', 'laboratory_name', 'product_tmpl_id']
    )
    if not data_prod:
        return pd.DataFrame(columns=['COD ART', 'ART DES', 'PROV DES', 'PRECIO VENTA', '_product_id'])

    df_prod = pd.DataFrame(data_prod)
    df_prod['tmpl_id'] = df_prod['product_tmpl_id'].apply(id_odoo)

    tmpl_ids = df_prod['tmpl_id'].dropna().unique().tolist()
    data_tmpl = odoo_search_read(
        'product.template',
        [('id', 'in', tmpl_ids), ('active', 'in', [True, False])],
        ['list_price_usd']
    )
    df_tmpl = pd.DataFrame(data_tmpl).rename(columns={'id': 'tmpl_id'}) if data_tmpl else \
        pd.DataFrame(columns=['tmpl_id', 'list_price_usd'])

    df = df_prod.merge(df_tmpl, on='tmpl_id', how='left')

    maeinv = pd.DataFrame({
        'COD ART':      df['barcode'].apply(lambda v: '' if v is False or v is None else str(v)),
        'ART DES':      df['name'],
        'PROV DES':     df['laboratory_name'].apply(limpiar_odoo),
        'PRECIO VENTA': pd.to_numeric(df['list_price_usd'], errors='coerce').fillna(0).round(2),
        '_product_id':  df['id'],
    })
    return maeinv


def completar_maeinv_con_productos_vendidos(df_maeinv, df_productos_facmes):
    """
    Si en FACMES aparece un producto (por id de product.product) que no está
    en df_maeinv (por ejemplo porque está archivado y obtener_maeinv solo
    trae activos), se agrega esa fila a MAEINV con sus mismos datos.
    """
    if df_productos_facmes is None or df_productos_facmes.empty:
        return df_maeinv

    ids_existentes = set(df_maeinv['_product_id'].dropna().tolist()) if '_product_id' in df_maeinv.columns else set()
    faltantes = df_productos_facmes[~df_productos_facmes['product_id_int'].isin(ids_existentes)].copy()
    faltantes = faltantes.drop_duplicates(subset=['product_id_int'])

    if faltantes.empty:
        return df_maeinv

    extra = pd.DataFrame({
        'COD ART':      faltantes['barcode'].apply(lambda v: '' if v is False or v is None else str(v)),
        'ART DES':      faltantes['name'],
        'PROV DES':     faltantes['laboratory_name'].apply(limpiar_odoo),
        'PRECIO VENTA': pd.to_numeric(faltantes['list_price_usd'], errors='coerce').fillna(0).round(2),
        '_product_id':  faltantes['product_id_int'],
    })
    return pd.concat([df_maeinv, extra], ignore_index=True)


# ─────────────────────────────────────────────
# PESTAÑA 2: MAECLI — clientes (excluye contactos etiquetados "Laboratorio")
# ─────────────────────────────────────────────

def obtener_maecli():
    """
    COD CLIENTE → id (res.partner)
    CLIENTE     → name
    DIRECCION   → contact_address_complete
    CIUDAD      → city
    TELEFONOS   → phone
    RIF         → "J" + vat

    category_id es un campo many2many (etiquetas de contacto). search_read
    devuelve solo los ids de las etiquetas, así que se resuelven aparte los
    nombres para poder incluir solo los contactos que tengan al menos una
    etiqueta "Cliente" o "Inactivo" (comparación sin tildes/mayúsculas).
    """
    data_partners = odoo_search_read(
        'res.partner', [],
        ['name', 'contact_address_complete', 'city', 'phone', 'vat', 'category_id']
    )
    if not data_partners:
        return pd.DataFrame(columns=['COD CLIENTE', 'CLIENTE', 'DIRECCION', 'CIUDAD', 'TELEFONOS', 'RIF'])

    df = pd.DataFrame(data_partners)

    todas_cat_ids = sorted({cid for lista in df['category_id'] for cid in (lista or [])})
    nombre_cat = {}
    if todas_cat_ids:
        data_cat = odoo_search_read('res.partner.category', [('id', 'in', todas_cat_ids)], ['name'])
        nombre_cat = {c['id']: c['name'] for c in data_cat}

    def tiene_tag_cliente_o_inactivo(lista_ids):
        for cid in (lista_ids or []):
            nombre = quitar_tildes(str(nombre_cat.get(cid, ''))).strip().lower()
            if 'cliente' in nombre or 'inactivo' in nombre:
                return True
        return False

    df['_es_cliente_o_inactivo'] = df['category_id'].apply(tiene_tag_cliente_o_inactivo)
    df = df[df['_es_cliente_o_inactivo']].copy()

    maecli = pd.DataFrame({
        'COD CLIENTE': df['id'],
        'CLIENTE':     df['name'],
        'DIRECCION':   df['contact_address_complete'].apply(lambda v: '' if v is False or v is None else v),
        'CIUDAD':      df['city'].apply(lambda v: '' if v is False or v is None else v),
        'TELEFONOS':   df['phone'].apply(lambda v: '' if v is False or v is None else v),
        'RIF':         df['vat'].apply(lambda v: '' if v is False or v is None else f"J{v}"),
    })

    # Eliminar duplicados por nombre de cliente (se conserva la primera aparición)
    maecli = maecli.drop_duplicates(subset=['CLIENTE'], keep='first').reset_index(drop=True)
    return maecli


# ─────────────────────────────────────────────
# PESTAÑA 3: FACMES — líneas de facturas y notas de crédito (devoluciones)
# ─────────────────────────────────────────────

def obtener_facmes(fecha_inicio, fecha_fin):
    """
    Notas de crédito: se incluyen todas las que tengan refund_cause
    distinto de vacío/False (no solo las que contengan "Devolución").

    COD ART      → barcode (product.product de la línea)
    COD CLIENTE  → id de partner_id de la factura/nota
    UNIDADES     → quantity de la línea; negativo si es Nota de Crédito
    TOTAL        → price_subtotal de la línea de factura/nota (account.move.line);
                   negativo si es Nota de Crédito. Si la factura/NC está en
                   Bolívares, price_subtotal se divide por la tasa de la
                   factura (os_currency_rate) para obtener el valor en USD,
                   redondeado a 2 decimales. Si ya está en USD, no se divide.
    FECHA VENTAS → invoice_date en formato aaaammdd (texto, sin separadores)
    F, G         → vacías
    H            → una única celda con la suma total de la columna D (TOTAL)

    Devuelve una tupla (facmes, df_productos_facmes). df_productos_facmes
    trae el detalle completo (id, barcode, name, laboratory_name,
    list_price_usd) de cada producto vendido en el período, para poder
    completar MAEINV con productos archivados que no estén en el catálogo
    activo (ver completar_maeinv_con_productos_vendidos).
    """
    cols_vacias = ['COD ART', 'COD CLIENTE', 'UNIDADES', 'TOTAL', 'FECHA VENTAS', 'F', 'G', 'H']
    cols_prod_vacias = ['product_id_int', 'barcode', 'name', 'laboratory_name', 'list_price_usd']

    domain_facturas = [
        ('move_type', '=', 'out_invoice'),
        ('state', '=', 'posted'),
        ('invoice_date', '>=', str(fecha_inicio)),
        ('invoice_date', '<=', str(fecha_fin)),
    ]
    domain_notas = [
        ('move_type', '=', 'out_refund'),
        ('state', '=', 'posted'),
        ('invoice_date', '>=', str(fecha_inicio)),
        ('invoice_date', '<=', str(fecha_fin)),
        ('refund_cause', 'not in', [False, '']),
    ]

    campos_move = ['move_type', 'invoice_date', 'partner_id', 'currency_id', 'os_currency_rate']
    data_facturas = odoo_search_read('account.move', domain_facturas, campos_move)
    data_notas    = odoo_search_read('account.move', domain_notas, campos_move)

    data_moves = (data_facturas or []) + (data_notas or [])
    if not data_moves:
        return pd.DataFrame(columns=cols_vacias), pd.DataFrame(columns=cols_prod_vacias)

    df_moves = pd.DataFrame(data_moves).rename(columns={'id': 'move_id_int'})

    move_ids = df_moves['move_id_int'].tolist()
    data_lineas = odoo_search_read(
        'account.move.line',
        [('move_id', 'in', move_ids), ('product_id', '!=', False)],
        ['move_id', 'product_id', 'quantity', 'price_subtotal']
    )
    if not data_lineas:
        return pd.DataFrame(columns=cols_vacias), pd.DataFrame(columns=cols_prod_vacias)

    df_lineas = pd.DataFrame(data_lineas)
    df_lineas['move_id_int']    = df_lineas['move_id'].apply(id_odoo)
    df_lineas['product_id_int'] = df_lineas['product_id'].apply(id_odoo)

    product_ids = list(set(df_lineas['product_id_int'].dropna().tolist()))
    data_prod = odoo_search_read(
        'product.product',
        [('id', 'in', product_ids), ('active', 'in', [True, False])],
        ['barcode', 'name', 'laboratory_name', 'product_tmpl_id']
    )
    df_prod = pd.DataFrame(data_prod).rename(columns={'id': 'product_id_int'}) if data_prod else \
        pd.DataFrame(columns=['product_id_int', 'barcode', 'name', 'laboratory_name', 'product_tmpl_id'])
    df_prod['tmpl_id'] = df_prod['product_tmpl_id'].apply(id_odoo)

    tmpl_ids = df_prod['tmpl_id'].dropna().unique().tolist()
    data_tmpl = odoo_search_read(
        'product.template',
        [('id', 'in', tmpl_ids), ('active', 'in', [True, False])],
        ['list_price_usd']
    )
    df_tmpl = pd.DataFrame(data_tmpl).rename(columns={'id': 'tmpl_id'}) if data_tmpl else \
        pd.DataFrame(columns=['tmpl_id', 'list_price_usd'])

    df_prod = df_prod.merge(df_tmpl, on='tmpl_id', how='left')
    df_prod['list_price_usd'] = pd.to_numeric(df_prod['list_price_usd'], errors='coerce').fillna(0)

    df = df_lineas.merge(df_moves, on='move_id_int', how='left')
    df = df.merge(df_prod[['product_id_int', 'barcode']], on='product_id_int', how='left')

    df['partner_id_num'] = df['partner_id'].apply(id_odoo)
    df['signo'] = np.where(df['move_type'] == 'out_refund', -1, 1)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    df['price_subtotal'] = pd.to_numeric(df['price_subtotal'], errors='coerce').fillna(0)
    df['fecha_aaaammdd'] = pd.to_datetime(df['invoice_date'], errors='coerce').dt.strftime('%Y%m%d')

    # Facturas/NC en Bolívares: price_subtotal viene en Bs, así que se divide
    # por la tasa de la factura (os_currency_rate) para obtener el valor
    # equivalente en USD. Si la factura ya está en USD, se deja tal cual.
    df['es_usd'] = df['currency_id'].apply(es_moneda_usd)
    df['os_currency_rate'] = pd.to_numeric(df['os_currency_rate'], errors='coerce')

    total_base = df['price_subtotal'] * df['signo']
    tasa_valida = df['os_currency_rate'].notna() & (df['os_currency_rate'] != 0)
    total_final = np.where(
        ~df['es_usd'] & tasa_valida,
        (total_base / df['os_currency_rate']).round(2),
        total_base
    )

    facmes = pd.DataFrame({
        'COD ART':      df['barcode'].apply(lambda v: '' if v is False or v is None else str(v)),
        'COD CLIENTE':  df['partner_id_num'],
        'UNIDADES':     df['quantity'] * df['signo'],
        'TOTAL':        total_final,
        'FECHA VENTAS': df['fecha_aaaammdd'],
    })

    df_productos_facmes = df_prod[cols_prod_vacias].drop_duplicates(subset=['product_id_int'])

    return facmes, df_productos_facmes


# ─────────────────────────────────────────────
# MOTOR DE EXCEL — 3 pestañas
# ─────────────────────────────────────────────

def _escribir_hoja_simple(workbook, sheet_name, df, header_format, data_format):
    """Escribe una hoja simple, ignorando cualquier columna interna (prefijo '_'),
    con todas las columnas centradas (header_format y data_format ya vienen centrados)."""
    columnas_visibles = [c for c in df.columns if not c.startswith('_')]

    worksheet = workbook.add_worksheet(sheet_name)
    for col_num, col_name in enumerate(columnas_visibles):
        worksheet.write(0, col_num, col_name, header_format)
    for row_num, (_, fila) in enumerate(df.iterrows()):
        for col_num, col_name in enumerate(columnas_visibles):
            worksheet.write(row_num + 1, col_num, fila[col_name], data_format)
    for col_num, col_name in enumerate(columnas_visibles):
        col_data = df[col_name].astype(str).fillna("")
        ancho = max(col_data.map(len).max() if len(col_data) else 0, len(col_name)) + 2
        worksheet.set_column(col_num, col_num, ancho)
    return worksheet


def _escribir_facmes(workbook, df_facmes, header_format_center, header_format_left,
                      data_format_center, num_format_total, bold_format_center):
    """
    Todas las columnas centradas, EXCEPTO la columna E (FECHA VENTAS), que
    queda con alineación normal (sin centrar).
    """
    worksheet = workbook.add_worksheet("FACMES")
    monto_format_center      = workbook.add_format({'num_format': num_format_total, 'align': 'center'})
    bold_monto_format_center = workbook.add_format({'num_format': num_format_total, 'bold': True, 'align': 'center'})
    total_format_center      = workbook.add_format({'align': 'center'})  # formato General (sin num_format)

    encabezados = ['COD ART', 'COD CLIENTE', 'UNIDADES', 'TOTAL', 'FECHA VENTAS', '', '', '']
    for col_num, value in enumerate(encabezados):
        if not value:
            continue
        fmt = header_format_left if col_num == 4 else header_format_center
        worksheet.write(0, col_num, value, fmt)

    for row_num, (_, fila) in enumerate(df_facmes.iterrows()):
        worksheet.write(row_num + 1, 0, fila['COD ART'], data_format_center)
        worksheet.write(row_num + 1, 1, fila['COD CLIENTE'], data_format_center)
        worksheet.write(row_num + 1, 2, fila['UNIDADES'], data_format_center)
        worksheet.write(row_num + 1, 3, fila['TOTAL'], total_format_center)
        worksheet.write(row_num + 1, 4, fila['FECHA VENTAS'])  # sin centrar (excepción pedida)
        # columnas F y G quedan vacías a propósito

    n = len(df_facmes)
    if n > 0:
        worksheet.write_formula(0, 7, f"=SUM(C2:C{n + 1})", bold_monto_format_center)  # columna H, solo esta celda
    else:
        worksheet.write(0, 7, 0, bold_monto_format_center)

    for col_num, ancho_col in [(0, 16), (1, 14), (2, 12), (3, 16), (4, 14), (7, 16)]:
        worksheet.set_column(col_num, col_num, ancho_col)

    return worksheet


def generar_excel_master_data(df_maeinv, df_maecli, df_facmes):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        header_format_center = workbook.add_format({'bold': True, 'align': 'center'})
        header_format_left   = workbook.add_format({'bold': True})
        data_format_center   = workbook.add_format({'align': 'center'})

        _escribir_hoja_simple(workbook, "MAEINV", df_maeinv, header_format_center, data_format_center)
        _escribir_hoja_simple(workbook, "MAECLI", df_maecli, header_format_center, data_format_center)
        _escribir_facmes(
            workbook, df_facmes, header_format_center, header_format_left,
            data_format_center, '#,##0.00', header_format_center
        )

    output.seek(0)
    return output.getvalue()


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def render_reporte(fecha_inicio, fecha_fin):
    st.header("🗂️ Master Data DROGUERÍA BLV, C.A.")

    for key, default in [
        ('md_excel_binario', None), ('md_nombre_archivo', None), ('_odoo_client', None),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    st.caption(
        f"📅 Rango de facturación: {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')} "
        f"· Fecha de extracción: {date.today().strftime('%d/%m/%Y')}"
    )

    if st.button("🚀 Generar Master Data", type="primary"):
        try:
            with st.spinner("Consultando catálogo de productos (MAEINV)..."):
                df_maeinv = obtener_maeinv()
            with st.spinner("Consultando clientes (MAECLI)..."):
                df_maecli = obtener_maecli()
            with st.spinner("Consultando facturas y notas de crédito (FACMES)..."):
                df_facmes, df_productos_facmes = obtener_facmes(fecha_inicio, fecha_fin)

            n_maeinv_antes = len(df_maeinv)
            df_maeinv = completar_maeinv_con_productos_vendidos(df_maeinv, df_productos_facmes)
            n_agregados = len(df_maeinv) - n_maeinv_antes
            if n_agregados > 0:
                st.caption(
                    f"ℹ️ {n_agregados} producto(s) vendido(s) en el período no estaban en el catálogo "
                    f"activo (posiblemente archivados) y se agregaron a MAEINV."
                )

            excel_bytes = generar_excel_master_data(df_maeinv, df_maecli, df_facmes)
            nombre_archivo = construir_nombre_archivo(fecha_inicio, fecha_fin)

            st.session_state.md_excel_binario  = excel_bytes
            st.session_state.md_nombre_archivo = nombre_archivo

            st.success(
                f"✅ Listo: {len(df_maeinv)} producto(s) · {len(df_maecli)} cliente(s) · "
                f"{len(df_facmes)} línea(s) de factura/NC."
            )
        except Exception as e:
            st.error(
                f"Error crítico: {e}\n\n"
                "Si el error menciona '503' o 'Service Unavailable', es una caída temporal del "
                "servidor de Odoo. Esperá unos segundos y volvé a presionar '🚀 Generar Master Data'."
            )

    if st.session_state.md_excel_binario:
        st.divider()
        st.download_button(
            label="📦 Descargar Master Data (Excel)",
            data=st.session_state.md_excel_binario,
            file_name=st.session_state.md_nombre_archivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
