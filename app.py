from flask import Flask, request, jsonify
from pymongo import MongoClient
import psycopg2
import psycopg2.extras
import os
import logging
from datetime import datetime

#------------------------------------
# Configuração de logs
#------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

#------------------------------------
# Conexão MongoDB
#-----------------------------------


# Obtenha a URI a partir da variável de ambiente
MONGO_URI = os.environ.get("MONGODB_URI")
mongo_client = MongoClient(MONGO_URI)

# Selecione o banco de dados e a coleção
db = mongo_client["Assuntos"]
collection = db["Crisp.Assuntos Crisp"]

#------------------------------------
# Conexão PostgreSQL
#------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_pg_connection():
    return psycopg2.connect(DATABASE_URL)

def salvar_no_postgresql(data):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crisp_eventos (
                website_id, event, timestamp,
                session_id,
                is_student, is_seller, is_team_member,
                is_web, mobile_app, blacklist_till,
                total_commission_earned, assunto,
                payload
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            data.get("website_id"),
            data.get("event"),
            data.get("timestamp"),
            data.get("data", {}).get("session_id"),
            # campos de data.data
            data.get("data", {}).get("data", {}).get("isStudent"),
            data.get("data", {}).get("data", {}).get("isSeller"),
            data.get("data", {}).get("data", {}).get("isTeamMember"),
            data.get("data", {}).get("data", {}).get("isWeb"),
            data.get("data", {}).get("data", {}).get("mobile_app"),
            data.get("data", {}).get("data", {}).get("blacklist_till"),
            data.get("data", {}).get("data", {}).get("total_commission_earned"),
            data.get("data", {}).get("data", {}).get("assunto"),
            psycopg2.extras.Json(data)
        ])
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"[PostgreSQL] Erro detalhado: {e}", flush=True)
        return False

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        # Recebe os dados JSON do webhook
        data = request.json
        
        # Insere os dados na coleção do MongoDB
        result = collection.insert_one(data)
        # Adiciona o _id ao retorno e converte para string
        data["_id"] = str(result.inserted_id)
        
        pg_ok = salvar_no_postgresql(data)
        if not pg_ok:
            logger.warning(f"[PostgreSQL] Falha silenciosa para o evento: {data.get('_id')}")

        # Retorna uma resposta de sucesso
        return jsonify({
            "status": "sucesso", 
            "data": data,
            "postgres": "ok" if pg_ok else "falha silenciosa"
            }), 200
    
    except Exception as e:
        # Em caso de erro, retorna uma resposta de erro
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  # Use a variável de ambiente PORT
    app.run(host='0.0.0.0', port=port)
