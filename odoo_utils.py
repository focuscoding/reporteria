import xmlrpc.client
import requests
import streamlit as st

# odoo_utils.py

class OdooClient:
    # Agregamos los parámetros individuales después de self
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password

        self.common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        self.uid = self.common.authenticate(db, username, password, {})

        if not self.uid:
            raise Exception("Error de autenticación en Odoo")

        self.models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

    def search_read(self, model, domain, fields):
        return self.models.execute_kw(
            self.db,
            self.uid,
            self.password,
            model,
            'search_read',
            [domain],
            {'fields': fields}
        )



def enviar_a_google_sheets(data):
    script_url = st.secrets["google"]["script_url"]
    try:
        response = requests.post(script_url, json={"action": "append_data", "data": data}, timeout=15)
        return response.json().get("success")
    except:
        return False