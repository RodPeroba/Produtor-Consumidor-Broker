import threading
import os
import json
from flask import Flask
from kafka import KafkaConsumer
from dotenv import load_dotenv
from mysql.connector import Connect

load_dotenv()
topic = os.getenv("TOPIC")
temperature_max = float(os.getenv("MAX_TEMPERATURE"))
temperature_min = float(os.getenv("MIN_TEMPERATURE"))
vibration_max = float(os.getenv("MAX_VIBRATION"))
vibration_min = float(os.getenv("MIN_VIBRATION"))

consumer_thread = None

app = Flask(__name__)

'''
    Função para conectar ao banco de dados MySQL
'''
def connect_to_database():
    connection = Connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )
    return connection

def create_table():
    connection = connect_to_database()
    cursor = connection.cursor()
    # Decimal é usado para ter precisão nos valores de temperatura e vibração 
    # Apenas 2 casas decimais por escolha arbitraria para o problema
    # Em casos reais, esse numero seria passado pela documentação do sensor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            temperatura DECIMAL(10, 2) NOT NULL,
            vibracao DECIMAL(10, 2) NOT NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    connection.commit()
    cursor.close()
    connection.close()
    return

'''
    Função para consumir as mensagens do Kafka e conferir se os valores de temperatura e vibração estão dentro dos limites aceitáveis
    Caso algum valor esteja fora do limite, será impresso no console uma mensagem de alerta
'''
def consume_messages():
    consumer = KafkaConsumer(topic, bootstrap_servers='kafka:9092')
    app.logger.info("Consumidor iniciado, aguardando mensagens...")
    connection = connect_to_database()
    cursor = connection.cursor()
    try:
        for message in consumer:
            dados = json.loads(message.value.decode('utf-8'))
            cursor.execute("INSERT INTO alerts (temperatura, vibracao) VALUES (%s, %s)", (dados['temperatura'], dados['vibracao']))
            connection.commit()
            # app.logger.info(f"Mensagem recebida: {message.value.decode('utf-8')}")
    except Exception as e:
        app.logger.error(f"Erro ao consumir mensagens: {e}")
    finally:
        consumer.close()
        app.logger.info("Consumidor finalizado.")
    return

'''
    Rota inicial, exibe um botão para ligar a máquina no contexto do problema
'''
@app.route("/")
def index():
    return "<button><a href='/startconsumer'>Iniciar Consumo de Dados</a></button>"


'''
    Rota para iniciar o consumo de mensagens do Kafka
    Semelhante a ligar uma máquina que consumirá continuamente as mensagens
'''
@app.route("/startconsumer")
def start_consumer():
    global consumer_thread
    # Evita gerar mais de uma thread por consumidor, uso de thread só para o site funcionar
    if consumer_thread is None or not consumer_thread.is_alive():
        # Consumirá as mensagens do Kafka, uso de threading pois o consumo de mensagem é continuo
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.start()
    return "Consumo de dados iniciado"

if __name__ == "__main__":
    create_table()
    app.run(host='0.0.0.0', port=5001, debug=True)