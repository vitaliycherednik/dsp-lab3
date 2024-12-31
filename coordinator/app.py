import os
import json
import pika
import uuid
from flask import Flask, request, jsonify
import logging
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionCoordinator:
    def __init__(self):
        self.host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )
        self.channel = self.connection.channel()
        
        # Черги для комунікації з сервісами
        self.channel.queue_declare(queue='prepare_queue')
        self.channel.queue_declare(queue='commit_queue')
        self.channel.queue_declare(queue='rollback_queue')
        
        # Черга для відповідей
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        
        self.responses = {}
        self.transaction_states = {}

    def on_response(self, ch, method, props, body):
        """Обробка відповідей від сервісів"""
        if props.correlation_id not in self.responses:
            self.responses[props.correlation_id] = []
        self.responses[props.correlation_id].append(json.loads(body))

    def prepare_phase(self, transaction_id, data):
        """Фаза підготовки"""
        logger.info(f"Starting prepare phase for transaction {transaction_id}")
        correlation_id = str(uuid.uuid4())
        
        # Відправляємо запит на підготовку обом сервісам
        self.channel.basic_publish(
            exchange='',
            routing_key='prepare_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=correlation_id,
                headers={'transaction_id': transaction_id}
            ),
            body=json.dumps(data)
        )
        
        # Чекаємо на відповіді від обох сервісів
        while len(self.responses.get(correlation_id, [])) < 2:
            self.connection.process_data_events()

            
        responses = self.responses.pop(correlation_id, [])
        logger.info(f"Received prepare responses for transaction {transaction_id}: {responses}")
        return all(r.get('prepared', False) for r in responses)

    def commit_phase(self, transaction_id):
        """Фаза підтвердження"""
        logger.info(f"Starting commit phase for transaction {transaction_id}")
        correlation_id = str(uuid.uuid4())
        
        # Відправляємо запит на підтвердження обом сервісам
        self.channel.basic_publish(
            exchange='',
            routing_key='commit_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=correlation_id,
                headers={'transaction_id': transaction_id}
            ),
            body=json.dumps({'action': 'commit'})
        )
        
        # Чекаємо на відповіді від обох сервісів
        while len(self.responses.get(correlation_id, [])) < 2:
            self.connection.process_data_events()

            
        responses = self.responses.pop(correlation_id, [])
        logger.info(f"Received commit responses for transaction {transaction_id}: {responses}")
        return all(r.get('committed', False) for r in responses)

    def rollback_phase(self, transaction_id):
        """Фаза відкату"""
        logger.info(f"Starting rollback phase for transaction {transaction_id}")
        correlation_id = str(uuid.uuid4())
        
        self.channel.basic_publish(
            exchange='',
            routing_key='rollback_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=correlation_id,
                headers={'transaction_id': transaction_id}
            ),
            body=json.dumps({'action': 'rollback'})
        )
        logger.info(f"Rollback initiated for transaction {transaction_id}")

coordinator = TransactionCoordinator()

@app.route('/transaction', methods=['POST'])
def process_transaction():
    transaction_id = str(uuid.uuid4())
    data = request.json
    
    logger.info(f"Starting transaction {transaction_id} with data: {data}")
    
    try:
        # Phase 1: Prepare
        logger.info(f"Phase 1: Preparing transaction {transaction_id}")
        if coordinator.prepare_phase(transaction_id, data):
            # Phase 2: Commit
            logger.info(f"Phase 2: Committing transaction {transaction_id}")
            if coordinator.commit_phase(transaction_id):
                logger.info(f"Transaction {transaction_id} completed successfully")
                return jsonify({
                    'status': 'success',
                    'transaction_id': transaction_id,
                    'message': 'Transaction completed successfully'
                })
            else:
                logger.error(f"Commit failed for transaction {transaction_id}")
                coordinator.rollback_phase(transaction_id)
                return jsonify({
                    'status': 'error',
                    'transaction_id': transaction_id,
                    'message': 'Commit failed, transaction rolled back'
                }), 500
        else:
            logger.error(f"Prepare phase failed for transaction {transaction_id}")
            coordinator.rollback_phase(transaction_id)
            return jsonify({
                'status': 'error',
                'transaction_id': transaction_id,
                'message': 'Prepare phase failed, transaction rolled back'
            }), 500
            
    except Exception as e:
        logger.error(f"Error processing transaction {transaction_id}: {str(e)}")
        coordinator.rollback_phase(transaction_id)
        return jsonify({
            'status': 'error',
            'transaction_id': transaction_id,
            'message': f'Transaction failed: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
