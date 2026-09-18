from flask import Flask
from kafka import KafkaProducer
import time
import threading

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:9092')
topico = 'dados-sensores'
temperature = 25.0
vibration = 0.5
time_interval = 1
producer_thread = None

'''
    Função para gerar os dados em JSON e enviar para o Kafka
'''

def generate_and_send_message():
    contador = 0
    while True:
        message = {
                    "temperatura": temperature,
                    "vibracao": vibration
                    }
        producer.send(topic=topico, value=str(message).encode('utf-8'))
        producer.flush()
        contador +=1
        time.sleep(time_interval)
    return 

@app.route("/")
def index():
    return "<button><a href='/startproducer'>Iniciar Geração de Dados</a></button>"


'''
    Rota para iniciar a geração de dados em JSON e envio para o Kafka
    Semelhante a ligar uma máquina que gerará continuamente as mensagens
'''
@app.route("/startproducer")
def generate_data():
    global producer_thread
    # Evita gerar mais de uma thread por produtor
    if producer_thread is None or not producer_thread.is_alive():
        # Gera a mensagem em JSON e envia para o kafka, uso de threading pois a geração de mensagem é continua
        producer_thread = threading.Thread(target=generate_and_send_message)
        producer_thread.start()
    return "Geração de dados iniciada"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
