import streamlit as st
import pandas as pd
import io
from odoo_utils import OdooClient


# ─────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────

def limpiar(val):
    return val[1] if isinstance(val, (list, tuple)) else val

def limpiar_barcode(val):
    if not val:
        return ""
    return str(val).split('.')[0].strip()


# ─────────────────────────────────────────────
# MOTOR DE EXCEL
# ─────────────────────────────────────────────

def motor_excel_extraccion(df_final):
    """
    Genera un Excel por laboratorio a partir del DataFrame extraído de Odoo.
    No aplica lógica de descuentos — exporta los datos tal cual vienen.
    Devuelve dict {lab: bytes}.
    """
    if df_final.empty:
        return {}

    diccionario_excels = {}

    for lab in df_final['laboratory_name'].unique():
        df_lab = df_final[df_final['laboratory_name'] == lab].copy()

        if df_lab.empty:
            continue

        for col in ['quantity', 'price_unit']:
            if col in df_lab.columns:
                df_lab[col] = df_lab[col].apply(
                    lambda x: x[0] if isinstance(x, (list, tuple)) else x
                )
                df_lab[col] = pd.to_numeric(df_lab[col], errors='coerce').fillna(0)

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
            'price_unit':          df_lab['price_unit'],
            'currency_id':         df_lab['currency_id'],
        })
        reporte['subtotal'] = reporte['quantity'] * reporte['price_unit']

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            reporte_export = reporte.drop(columns=['currency_id'], errors='ignore')
            reporte_export.to_excel(
                writer, index=False, sheet_name='Extracción',
                startrow=1, header=False
            )
            workbook  = writer.book
            worksheet = writer.sheets['Extracción']

            header_format = workbook.add_format({'bold': True, 'border': 0})
            dollar_format = workbook.add_format({'num_format': '$#,##0.00'})
            bs_format     = workbook.add_format({'num_format': '"Bs." #,##0.00'})
            bold_format   = workbook.add_format({'bold': True})

            encabezados = [
                'Fecha Factura', 'ID Cliente', 'Cliente', 'Nro. Factura',
                'Código de Barras', 'Descripción', 'Laboratorio',
                'Código Laboratorio', 'Cantidad', 'Precio Unitario', 'Subtotal',
            ]
            for col_num, value in enumerate(encabezados):
                worksheet.write(0, col_num, value, header_format)

            # Fórmula subtotal
            for row_num in range(len(reporte)):
                worksheet.write_formula(
                    row_num + 1, 10,
                    f'=I{row_num + 2}*J{row_num + 2}'
                )

            # Total al pie
            last_row = len(reporte) + 1
            worksheet.write(last_row, 9, "Total", bold_format)
            worksheet.write_formula(
                last_row, 10,
                f"=SUM(K2:K{last_row})",
                bold_format
            )

            # Formato moneda por fila
            fmt = bs_format
            for row_num, moneda in enumerate(reporte['currency_id'], start=1):
                fmt = dollar_format if str(moneda).lower() in ['usd', 'dolares', '$'] else bs_format
                worksheet.conditional_format(
                    row_num, 9, row_num, 9,
                    {'type': 'no_errors', 'format': fmt}
                )
                worksheet.conditional_format(
                    row_num, 10, row_num, 10,
                    {'type': 'no_errors', 'format': fmt}
                )
            worksheet.conditional_format(
                last_row, 10, last_row, 10,
                {'type': 'no_errors', 'format': fmt}
            )

            # Ancho de columnas
            for i, col in enumerate(reporte_export.columns):
                col_data = reporte_export[col].astype(str).fillna('')
                worksheet.set_column(
                    i, i,
                    max(col_data.map(len).max(), len(encabezados[i] if i < len(encabezados) else col)) + 2
                )

        output.seek(0)
        diccionario_excels[lab] = output.getvalue()

    return diccionario_excels


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def render_extraccion_general(fecha_inicio, fecha_fin):
    st.header("📦 Extracción General")

    # Estado de sesión
    for key, default in [
        ('eg_df_resultado', None),
        ('eg_archivos_binarios', {}),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    # ── BOTÓN DE EJECUCIÓN ───────────────────────────────────────────
    if st.button("🚀 Extraer datos", type="primary"):
        try:
            config = st.secrets["odoo_bd1"]
            client = OdooClient(
                config["url"], config["db"],
                config["username"], config["password"]
            )

            domain = [
                ('date', '>=', str(fecha_inicio)),
                ('date', '<=', str(fecha_fin)),
                ('move_type', '=', 'out_invoice'),
                ('parent_state', '=', 'posted'),
                ('move_name', 'not ilike', 'ND%'),
                ('product_id', '!=', False),
                ('quantity', '>', 0),
            ]

            with st.spinner("Consultando Odoo..."):
                data_lineas = client.search_read(
                    'account.move.line', domain,
                    ['move_id', 'product_id', 'name', 'quantity', 'price_unit']
                )
                if not data_lineas:
                    st.warning("No hay datos para este período.")
                    return

                df_lineas = pd.DataFrame(data_lineas)

                move_ids    = list({x[0] for x in df_lineas['move_id']    if isinstance(x, list)})
                product_ids = list({x[0] for x in df_lineas['product_id'] if isinstance(x, list)})

                df_moves = pd.DataFrame(client.search_read(
                    'account.move', [('id', 'in', move_ids)],
                    ['invoice_date', 'partner_id', 'invoice_number_next', 'currency_id']
                )).rename(columns={'id': 'move_id_int'})

                df_prods = pd.DataFrame(client.search_read(
                    'product.product', [('id', 'in', product_ids)],
                    ['laboratory_name', 'supplier_code', 'barcode']
                )).rename(columns={'id': 'product_id_int'})

                df_lineas['move_id_int']    = df_lineas['move_id'].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
                df_lineas['product_id_int'] = df_lineas['product_id'].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )

                df_final = df_lineas.merge(df_moves, on='move_id_int', how='left')
                df_final = df_final.merge(df_prods,  on='product_id_int', how='left')

                # Construir resultado limpio
                res = pd.DataFrame({
                    'invoice_date':        pd.to_datetime(df_final['invoice_date']).dt.strftime('%d/%m/%Y'),
                    'partner_id_num':      df_final['partner_id'].apply(
                        lambda x: x[0] if isinstance(x, (list, tuple)) else x
                    ),
                    'partner_id':          df_final['partner_id'].apply(limpiar),
                    'invoice_number_next': df_final['invoice_number_next'],
                    'barcode':             df_final['barcode'].apply(limpiar_barcode),
                    'name':                df_final['name'],
                    'laboratory_name':     df_final['laboratory_name'].apply(limpiar),
                    'supplier_code':       df_final['supplier_code'].apply(
                        lambda x: '' if x is False or x is None else str(x)
                    ),
                    'quantity':            df_final['quantity'],
                    'price_unit':          df_final['price_unit'],
                    'currency_id':         df_final['currency_id'].apply(limpiar),
                })

                st.session_state.eg_df_resultado      = res
                st.session_state.eg_archivos_binarios = motor_excel_extraccion(res)
                st.rerun()

        except Exception as e:
            st.error(f"Error: {e}")

    # ── RENDERIZADO ──────────────────────────────────────────────────
    if st.session_state.eg_df_resultado is not None:
        df_display = st.session_state.eg_df_resultado

        col1, _ = st.columns([1, 4])
        with col1:
            if st.button("🗑️ Limpiar"):
                st.session_state.eg_df_resultado      = None
                st.session_state.eg_archivos_binarios = {}
                st.rerun()

        st.success(f"✅ Extracción completada: {len(df_display)} registros.")
        st.dataframe(df_display, use_container_width=True)

        # ── DESCARGAS ────────────────────────────────────────────────
        if st.session_state.eg_archivos_binarios:
            st.divider()
            st.write("### 📥 Descargar por Laboratorio")
            items = list(st.session_state.eg_archivos_binarios.items())

            for i in range(0, len(items), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(items):
                        lab, excel_data = items[i + j]
                        safe_lab = (
                            lab.replace(" ", "_").replace("/", "").replace("\\", "")
                            .replace(":", "").replace("á", "a").replace("é", "e")
                            .replace("í", "i").replace("ó", "o").replace("ú", "u")
                            .replace("Á", "A").replace("É", "E").replace("Í", "I")
                            .replace("Ó", "O").replace("Ú", "U").replace("ü", "u")
                            .replace("Ü", "U").replace("ñ", "n").replace("Ñ", "N")
                        )
                        with cols[j]:
                            st.download_button(
                                label=f"📦 {lab}",
                                data=excel_data,
                                file_name=(
                                    f"{safe_lab}_Extraccion_General_"
                                    f"{fecha_inicio.strftime('%d-%m-%Y')}_al_"
                                    f"{fecha_fin.strftime('%d-%m-%Y')}.xlsx"
                                ),
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"eg_dl_{lab}_{i+j}".replace(" ", "_")
                            )