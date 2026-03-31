import streamlit as st
import xmlrpc.client
import pandas as pd
import io
from datetime import date, timedelta
import unicodedata
import urllib.parse
from odoo_utils import OdooClient # Mantiene la importación modular

# -----------------------------
# FUNCIONES AUXILIARES
# -----------------------------

def procesar_facturas(data):
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["Moneda"] = df["currency_id"].apply(lambda x: x[1] if x else "")

    def obtener_impuesto(row):
        if row["Moneda"] == "Dolares":
            return row.get("amount_tax_usd", 0) or 0
        else:
            return row.get("amount_tax_bs", 0) or 0

    df["Impuesto"] = df.apply(obtener_impuesto, axis=1)
    df["Total Gravado"] = df["Impuesto"] / 0.16
    df["Exento"] = df["iva_exempt"].fillna(0)
    df["Total"] = df["Exento"] + df["Total Gravado"] + (df["Impuesto"] * 0.25)

    # Nota: Se usa "RNC" para capturar RNCVTA según el original
    mask_rnc = df["name"].str.contains("RNC", case=False, na=False)
    df.loc[mask_rnc, "Exento"] = -df.loc[mask_rnc, "Exento"]
    df.loc[mask_rnc, "Total Gravado"] = -df.loc[mask_rnc, "Total Gravado"]
    df.loc[mask_rnc, "Impuesto"] = -df.loc[mask_rnc, "Impuesto"]
    df.loc[mask_rnc, "Total"] = -df.loc[mask_rnc, "amount_total"]

    df_final = pd.DataFrame({
        "Empresa": "BLV",
        "Número": df["name"],
        "Fecha": df["invoice_date"],
        "Nro. Factura": df["invoice_number_next"],
        "Cliente": df["partner_id"].apply(lambda x: x[1] if x else ""),
        "Exento": df["Exento"],
        "Total Gravado": df["Total Gravado"],
        "Impuesto": df["Impuesto"],
        "Total": df["Total"],
        "Moneda": df["Moneda"]
    })

    return df_final

def calcular_resumen(df):
    if df.empty:
        return {}
    resumen = (
        df.groupby(["Empresa", "Moneda"])["Total"]
        .sum()
        .round(2)
        .reset_index()
    )
    resumen_dict = {}
    for _, row in resumen.iterrows():
        resumen_dict[(row["Empresa"], row["Moneda"])] = row["Total"]
    return resumen_dict

def generar_excel_formateado(df):
    resumen = calcular_resumen(df)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Reporte")
        workbook  = writer.book
        worksheet = writer.sheets["Reporte"]

        header_format = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter"})
        dollar_format = workbook.add_format({"num_format": "$#,##0.00"})
        bs_format = workbook.add_format({"num_format": '"Bs." #,##0.00'})

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        for row_num, moneda in enumerate(df["Moneda"], start=1):
            fmt = dollar_format if str(moneda).lower() == "dolares" else bs_format
            for col in range(5, 9):
                val = df.iloc[row_num-1, col]
                try:
                    val_num = float(val) if pd.notna(val) else 0
                except:
                    val_num = 0
                worksheet.write_number(row_num, col, val_num, fmt)
            worksheet.write(row_num, 9, df.iloc[row_num-1, 9])

        for i, col in enumerate(df.columns):
            column_data = df[col].astype(str).fillna("")
            max_len = max(column_data.map(len).max(), len(str(col)))
            worksheet.set_column(i, i, max_len + 3)

        bold_format = workbook.add_format({"bold": True})
        worksheet.write("L1", "BLV", bold_format)
        worksheet.write("L2", "Bolívares")
        worksheet.write("L3", "Dolares")
        worksheet.write("L4", "CRLV", bold_format)
        worksheet.write("L5", "Dolares")

        worksheet.write("M1", "Monto", bold_format)
        worksheet.write_number("M2", resumen.get(("BLV", "Bolívares"), 0), bs_format)
        worksheet.write_number("M3", resumen.get(("BLV", "Dolares"), 0), dollar_format)
        worksheet.write_number("M5", resumen.get(("CRLV", "Dolares"), 0), dollar_format)    

        worksheet.set_column("L:M", 15)
    return output.getvalue()

def limpiar_nombre(nombre):
    nombre = unicodedata.normalize('NFKD', nombre).encode('ASCII', 'ignore').decode('ASCII')
    nombre = nombre.replace(" ", "_")
    return nombre

def formato_moneda(valor, simbolo=""):
    if valor is None: valor = 0
    texto = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{simbolo} {texto}"

def construir_resumen_correo(resumen):
    blv_bs = formato_moneda(resumen.get(("BLV", "Bolívares"), 0), "Bs.")
    blv_usd = formato_moneda(resumen.get(("BLV", "Dolares"), 0), "$")
    crlv_usd = formato_moneda(resumen.get(("CRLV", "Dolares"), 0), "$")

    return f"""Espero estén bien.

Comparto relación de la semana pasada.

BLV 
Bolívares: {blv_bs}
Dólares: {blv_usd}

CRLV
Dólares: {crlv_usd}

Saludos,"""

# -----------------------------
# FUNCIÓN PRINCIPAL RENDERIZADO
# -----------------------------

def render_reporte(fecha_inicio, fecha_fin):
    st.subheader("📊 Facturación Farmago")

    if "df_farmago" not in st.session_state:
        st.session_state.df_farmago = None
    if "nombre_archivo" not in st.session_state:
        st.session_state.nombre_archivo = ""

    if st.button("🔍 Consultar Facturas Farmago", type="primary"):
        try:
            # --- CONEXIÓN BD1 (BLV) ---
            config = st.secrets["odoo_bd1"]
            client = OdooClient(config["url"], config["db"], config["username"], config["password"])

            domain = [
                ('move_type', 'in', ['out_invoice', 'out_refund']),
                ('invoice_partner_display_name', '=', 'FARMACIA FARMAGO, C.A.'),
                ('invoice_date', '>=', str(fecha_inicio)),
                ('invoice_date', '<=', str(fecha_fin)),
                ('state', '=', 'posted'),
                ('payment_state', '!=', 'reversed'),
                ('payment_state', '!=','paid')# Nueva condición
            ]

            fields_bd1 = ['name','invoice_date','invoice_number_next','partner_id',
                          'iva_exempt','amount_tax_usd','amount_tax_bs','currency_id','amount_total']

            with st.spinner("Consultando Odoo BD1..."):
                data_bd1 = client.search_read('account.move', domain, fields_bd1)
                df_bd1 = procesar_facturas(data_bd1)

            # --- CONEXIÓN BD2 (CRLV) ---
            config2 = st.secrets["odoo_bd2"]
            client2 = OdooClient(config2["url"], config2["db"], config2["username"], config2["password"])

            fields_bd2 = ['name', 'invoice_date', 'invoice_number_next', 'partner_id',
                          'amount_tax', 'subtotal_discount_rate', 'total_discount_rate', 
                          'currency_id', 'rate']

            with st.spinner("Consultando Odoo BD2..."):
                data_bd2 = client2.search_read('account.move', domain, fields_bd2)

            if data_bd2:
                df_raw_bd2 = pd.DataFrame(data_bd2)
                tasa = df_raw_bd2["rate"].replace(0, 1) if "rate" in df_raw_bd2.columns else 1
                
                # Fórmulas Originales BD2
                gravado_abs = (df_raw_bd2["subtotal_discount_rate"] - df_raw_bd2["total_discount_rate"]).abs()
                impuesto_abs = (df_raw_bd2["amount_tax"] / tasa).abs()
                subtotal_abs = df_raw_bd2["subtotal_discount_rate"].abs()
                exento_abs = (subtotal_abs - gravado_abs - impuesto_abs -  df_raw_bd2["total_discount_rate"]).clip(lower=0)
                
                is_nc = df_raw_bd2["name"].str.contains("NC", case=False, na=False)
                total_abs = pd.Series(0.0, index=df_raw_bd2.index)
                total_abs[~is_nc] = exento_abs[~is_nc] + gravado_abs[~is_nc] + (impuesto_abs[~is_nc] * 0.25)
                total_abs[is_nc] = exento_abs[is_nc] + gravado_abs[is_nc] + impuesto_abs[is_nc]

                df_bd2_final = pd.DataFrame({
                    "Empresa": "CRLV",
                    "Número": df_raw_bd2["name"],
                    "Fecha": df_raw_bd2["invoice_date"],
                    "Nro. Factura": df_raw_bd2["invoice_number_next"],
                    "Cliente": df_raw_bd2["partner_id"].apply(lambda x: x[1] if x else ""),
                    "Exento": exento_abs,
                    "Total Gravado": gravado_abs,
                    "Impuesto": impuesto_abs,
                    "Total": total_abs,
                    "Moneda": "Dolares"
                })

                cols_calc = ["Exento", "Total Gravado", "Impuesto", "Total"]
                df_bd2_final.loc[is_nc, cols_calc] *= -1
                df_bd2_final[cols_calc] = df_bd2_final[cols_calc].round(2)

                st.session_state.df_farmago = pd.concat([df_bd1, df_bd2_final], ignore_index=True)
            else:
                st.session_state.df_farmago = df_bd1

            if not st.session_state.df_farmago.empty:
                nombre = f"Relacion_Farmago_del_{fecha_inicio.strftime('%d-%m-%Y')}_al_{fecha_fin.strftime('%d-%m-%Y')}.xlsx"
                st.session_state.nombre_archivo = limpiar_nombre(nombre)
                st.success("✅ Datos cargados correctamente.")

        except Exception as e:
            st.error(f"Ocurrió un error: {str(e)}")

    # -----------------------------
    # MOSTRAR RESULTADOS Y FILTROS
    # -----------------------------
    if st.session_state.df_farmago is not None:
        st.divider()
        col_blv, col_crlv = st.columns(2)

        with col_blv:
            st.subheader("🏢 BLV")
            blv_nd_all = st.checkbox("Excluir todas las ND (BLV)", key="blv_nd_f")
            blv_nd_txt = st.text_input("ND específicas (BLV)", key="blv_nd_txt_f", disabled=blv_nd_all)
            blv_nc_all = st.checkbox("Excluir todas las NC (BLV)", key="blv_nc_f")
            blv_nc_txt = st.text_input("NC específicas (BLV)", key="blv_nc_txt_f", disabled=blv_nc_all)

        with col_crlv:
            st.subheader("🏢 CRLV")
            crlv_nd_all = st.checkbox("Excluir todas las ND (CRLV)", key="crlv_nd_f")
            crlv_nd_txt = st.text_input("ND específicas (CRLV)", key="crlv_nd_txt_f", disabled=crlv_nd_all)
            crlv_nc_all = st.checkbox("Excluir todas las NC (CRLV)", key="crlv_nc_f")
            crlv_nc_txt = st.text_input("NC específicas (CRLV)", key="crlv_nc_txt_f", disabled=crlv_nc_all)

        df_filtrado = st.session_state.df_farmago.copy()

        def aplicar_filtros(df, emp, nd_all, nd_txt, nc_all, nc_txt):
            mask_emp = df["Empresa"] == emp
            if nd_all:
                df = df[~(mask_emp & df["Número"].str.contains("ND", case=False, na=False))]
            elif nd_txt:
                items = [x.strip() for x in nd_txt.split(",") if x.strip()]
                mask = mask_emp & df["Número"].str.contains("ND", case=False, na=False) & \
                       df["Nro. Factura"].astype(str).apply(lambda x: any(e in x for e in items))
                df = df[~mask]
            
            if nc_all:
                df = df[~(mask_emp & df["Número"].str.contains("NC|RNC", case=False, na=False))]
            elif nc_txt:
                items = [x.strip() for x in nc_txt.split(",") if x.strip()]
                mask = mask_emp & df["Número"].str.contains("NC|RNC", case=False, na=False) & \
                       df["Nro. Factura"].astype(str).apply(lambda x: any(e in x for e in items))
                df = df[~mask]
            return df

        df_filtrado = aplicar_filtros(df_filtrado, "BLV", blv_nd_all, blv_nd_txt, blv_nc_all, blv_nc_txt)
        df_filtrado = aplicar_filtros(df_filtrado, "CRLV", crlv_nd_all, crlv_nd_txt, crlv_nc_all, crlv_nc_txt)

        resumen = calcular_resumen(df_filtrado)
        c1, c2, c3 = st.columns(3)
        c1.metric("BLV - Bs.", formato_moneda(resumen.get(("BLV", "Bolívares"), 0), "Bs."))
        c2.metric("BLV - USD", formato_moneda(resumen.get(("BLV", "Dolares"), 0), "$"))
        c3.metric("CRLV - USD", formato_moneda(resumen.get(("CRLV", "Dolares"), 0), "$"))

        st.download_button(
            label="⬇️ Descargar Excel",
            data=generar_excel_formateado(df_filtrado),
            file_name=st.session_state.nombre_archivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Mailto con múltiples destinatarios y CC del código original
        asunto = urllib.parse.quote(st.session_state.nombre_archivo.replace("_"," ").replace(".xlsx", ""))
        cuerpo = urllib.parse.quote(construir_resumen_correo(resumen))
        mailto_link = f"mailto:mramos.farmago@gmail.com;staddeo@drogueriablv.com?cc=vromero@drogueriablv.com&subject={asunto}&body={cuerpo}"
        st.link_button("📧 Crear correo con resumen", mailto_link)
