import requests
import logging


logger = logging.getLogger(__file__)


class AlarmManager:
    def upsert_alarm(self, topic, payload):
        topic_parts = topic.split('/')
        if len(topic_parts) != 4 or topic_parts[0] != 'alarms':
            logger.error('Invalid topic format')

        hospital_id, ward_id, exam_id = topic_parts[1], topic_parts[2], topic_parts[3]

        if not hospital_id or not ward_id or not exam_id:
            logger.error('Invalid topic - hospital_id, ward_id, or exam_id is missing')

        api_url = 'https://iomt.karina-huinno.tk/webhook/alarms'
        headers = {
            'Content-Type': 'application/json',
        }

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            if response.status_code == 200:
                api_response = response.json()
                return api_response
            else:
                logger.error(f'Failed to upsert alarm. Status code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.error(f'Error during API call: {e}')
