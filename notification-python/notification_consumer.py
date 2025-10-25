import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
import os
from datetime import datetime

# Configurações do Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'notifications'
GROUP_ID = 'email-notification-group'

# Configurações de Email (usando variáveis de ambiente)
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', 'seu-email@gmail.com')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', 'sua-senha')
FROM_EMAIL = os.getenv('FROM_EMAIL', SMTP_USER)

def send_email(to_email, subject, body):
    """
    Envia um email usando SMTP
    """
    try:
        # Criar mensagem
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Adicionar corpo do email
        msg.attach(MIMEText(body, 'html'))
        
        # Conectar ao servidor SMTP e enviar
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        print(f'✅ Email enviado para {to_email}')
        return True
    except Exception as e:
        print(f'❌ Erro ao enviar email: {str(e)}')
        return False

def process_notification(notification):
    """
    Processa uma notificação e envia o email
    """
    try:
        order_id = notification.get('orderId')
        customer_id = notification.get('customerId')
        notification_type = notification.get('type')
        message = notification.get('message')
        timestamp = notification.get('timestamp')
        
        print(f'\n📧 Processando notificação:')
        print(f'  - Pedido: {order_id}')
        print(f'  - Cliente: {customer_id}')
        print(f'  - Tipo: {notification_type}')
        print(f'  - Mensagem: {message}')
        
        # Email do cliente (simulado - em produção viria do banco de dados)
        customer_email = f'cliente{customer_id}@example.com'
        
        # Criar conteúdo do email
        subject = f'Atualização do Pedido #{order_id}'
        body = f"""
        <html>
        <body>
            <h2>Olá Cliente #{customer_id}!</h2>
            <p>{message}</p>
            <hr>
            <p><strong>Detalhes da Notificação:</strong></p>
            <ul>
                <li>Pedido: #{order_id}</li>
                <li>Tipo: {notification_type}</li>
                <li>Data: {timestamp}</li>
            </ul>
            <p>Obrigado por escolher nossos serviços!</p>
        </body>
        </html>
        """
        
        # Enviar email
        send_email(customer_email, subject, body)
        
    except Exception as e:
        print(f'❌ Erro ao processar notificação: {str(e)}')

def main():
    """
    Função principal que consome mensagens do Kafka
    """
    print(f'🚀 Iniciando consumidor de notificações...')
    print(f'📡 Conectando ao Kafka em {KAFKA_BROKER}')
    print(f'📬 Aguardando mensagens do tópico "{TOPIC}"...\n')
    
    # Criar consumidor Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Processar mensagens
    try:
        for message in consumer:
            notification = message.value
            process_notification(notification)
    except KeyboardInterrupt:
        print('\n⏸️  Encerrando consumidor...')
    finally:
        consumer.close()
        print('👋 Consumidor encerrado')

if __name__ == '__main__':
    main()
