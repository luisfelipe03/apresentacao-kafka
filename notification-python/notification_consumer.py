import os
from confluent_kafka import Consumer
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

def send_email_notification(subject, message, to_email=None):
    """
    Envia email de notificação usando configurações SMTP
    """
    try:
        # Configurações SMTP do .env
        smtp_server = os.getenv('SMTP_SERVER')
        smtp_port = int(os.getenv('SMTP_PORT'))
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        from_email = os.getenv('FROM_EMAIL')

        # Se não especificado, enviar para o mesmo email remetente
        if to_email is None:
            to_email = from_email

        # Criar mensagem de email
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        # Corpo do email
        msg.attach(MIMEText(message, 'plain'))

        # Conectar e enviar email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Habilitar segurança
        server.login(smtp_user, smtp_password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()

        print(f"Email enviado com sucesso para: {to_email}")
        return True

    except Exception as e:
        print(f"Erro ao enviar email: {str(e)}")
        return False

def main():
    # Configuração do consumidor Kafka
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'notification_group',
        'auto.offset.reset': 'earliest'
    }

    # Criação do consumidor Kafka
    consumer = Consumer(consumer_config)

    # Inscrição no tópico de notificações
    consumer.subscribe(['notifications'])

    print("Consumidor de notificações iniciado. Aguardando mensagens...")

    try:
        while True:
            # Poll por novas mensagens
            msg = consumer.poll(1.0)  # Timeout de 1 segundo

            if msg is None:
                continue  # Nenhuma mensagem recebida

            if msg.error():
                print(f"Erro no consumidor: {msg.error()}")
                continue

            # Processamento da mensagem recebida
            notification = msg.value().decode('utf-8')
            print(f"Notificação recebida: {notification}")
            # Enviar email de notificação
            send_email_notification("Nova Notificação", notification)

    except KeyboardInterrupt:
        print("Encerrando o consumidor...")

    finally:
        # Fechamento do consumidor
        consumer.close()
if __name__ == "__main__":
    main()
