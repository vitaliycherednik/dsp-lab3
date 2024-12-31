import os
import json
import pika
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(os.getenv('SERVICE_NAME', 'service'))

class TransactionParticipant:
    def __init__(self):
        self.host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
        self.service_name = os.getenv('SERVICE_NAME', 'service')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )
        self.channel = self.connection.channel()

        # Створення черг
        self.channel.queue_declare(queue='prepare_queue')
        self.channel.queue_declare(queue='commit_queue')
        self.channel.queue_declare(queue='rollback_queue')

        # Підписка на повідомлення
        self.channel.basic_consume(
            queue='prepare_queue',
            on_message_callback=self.handle_prepare
        )
        self.channel.basic_consume(
            queue='commit_queue',
            on_message_callback=self.handle_commit
        )
        self.channel.basic_consume(
            queue='rollback_queue',
            on_message_callback=self.handle_rollback
        )

        self.prepared_transactions = {}

    def handle_prepare(self, ch, method, props, body):
        transaction_id = props.headers.get('transaction_id')
        data = json.loads(body)

        logger.info(f"Preparing transaction {transaction_id}")

        # Симуляція підготовки транзакції
        try:
            # Зберігаємо дані транзакції
            self.prepared_transactions[transaction_id] = data
            success = True
        except Exception as e:
            success = False
            logger.error(f"Prepare failed: {str(e)}")

        # Відправляємо відповідь
        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=json.dumps({'prepared': success, 'service': self.service_name})
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_commit(self, ch, method, props, body):
        transaction_id = props.headers.get('transaction_id')
        logger.info(f"Committing transaction {transaction_id}")

        success = False
        if transaction_id in self.prepared_transactions:
            try:
                # Симуляція збереження даних
                data = self.prepared_transactions[transaction_id]
                # Тут може бути реальна логіка збереження даних
                del self.prepared_transactions[transaction_id]
                success = True
            except Exception as e:
                logger.error(f"Commit failed: {str(e)}")

        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=json.dumps({'committed': success, 'service': self.service_name})
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_rollback(self, ch, method, props, body):
        transaction_id = props.headers.get('transaction_id')
        logger.info(f"Rolling back transaction {transaction_id}")

        if transaction_id in self.prepared_transactions:
            del self.prepared_transactions[transaction_id]

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        logger.info(f"Starting {self.service_name}")
        self.channel.start_consuming()

if __name__ == '__main__':
    service2 = TransactionParticipant()
    service2.run()
