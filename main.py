from instances import *
import datetime
from datetime import timedelta
import logging
import configparser
import os
from model import Agent
from magafon_agents import agents_space
import concurrent.futures
import re
from pymongo import MongoClient


config = configparser.RawConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

LOG_FORMAT = config['logging']['format']
LOG_PATH = config['logging']['path']
LOG_LEVEL = config['logging']['level']

result_path = config['options']['result_path']

result_array = []

logging.addLevelName(logging.DEBUG, "\033[1;34m{}\033[1;0m".format(logging.getLevelName(logging.DEBUG)))
logging.addLevelName(logging.INFO, "\033[1;32m{}\033[1;0m".format(logging.getLevelName(logging.INFO)))
logging.addLevelName(logging.WARNING, "\033[1;33m{}\033[1;0m".format(logging.getLevelName(logging.WARNING)))
logging.addLevelName(logging.ERROR, "\033[1;31m{}\033[1;0m".format(logging.getLevelName(logging.ERROR)))
logging.addLevelName(logging.CRITICAL, "\033[1;41m{}\033[1;0m".format(logging.getLevelName(logging.CRITICAL)))

formatter = logging.Formatter(LOG_FORMAT)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
file_handler = logging.FileHandler(LOG_PATH)
file_handler.setFormatter(formatter)

logger = logging.getLogger('megafon_migrator_to_s3v3')
logger.setLevel(LOG_LEVEL)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


try:
    MAX_WORKERS = int(config['common']['max_workers'])
except (KeyError, ValueError):
    MAX_WORKERS = 10


class DialogRepositoryEntity:

    @staticmethod
    def pid_locker():
        if os.access(os.path.expanduser("~/.lockfile.megafon_v3s3_migrator.lock"), os.F_OK):
            pidfile = open(os.path.expanduser("~/.lockfile.megafon_v3s3_migrator.lock"), "r")
            pidfile.seek(0)
            old_pd = pidfile.readline()
            if os.path.exists("/proc/%s" % old_pd):
                exit(1)
            else:
                os.remove(os.path.expanduser("~/.lockfile.megafon_v3s3_migrator.lock"))
        pidfile = open(os.path.expanduser("~/.lockfile.megafon_v3s3_migrator.lock"), "w")
        pidfile.write("%s" % os.getpid())
        pidfile.close()

    @staticmethod
    def mongo_connect():
        try:
            mongo = MongoClient(config['source']['mongo'])
            mongo_db_statistics = mongo.statistics
            return mongo_db_statistics.statistic_dialog_report
        except Exception as e:
            logging.info(e, exc_info=True)
            logging.info('Mongo connection false')

    @staticmethod
    def get_bucket(agent_uuid):
        agent = session.query(Agent).filter_by(uuid=agent_uuid).first()
        if not agent:
            logger.info(f'Agent not found: {agent_uuid}')
            exit(2)
        bucket = agent.bucket_name
        return bucket

    @staticmethod
    def get_dates():
        data_from = config['options']['datetime_from']
        data_to = config['options']['datetime_to']
        try:
            data_from = datetime.datetime.strptime(data_from, "%Y-%m-%d %H:%M:%S %z")
            data_to = datetime.datetime.strptime(data_to, "%Y-%m-%d %H:%M:%S %z")
        except Exception as e:
            logger.info(f'Cant convert format {data_from} and {data_to} with exception {e}')
            day_today = datetime.datetime.now().date()
            data_to = datetime.datetime(year=day_today.year, month=day_today.month, day=day_today.day, hour=23,
                                        minute=59,
                                        second=00)
            data_from = data_to - timedelta(days=1)
        return data_from, data_to

    @staticmethod
    def get_result(call_result):
        if call_result:
            if re.search(r'неявное согласие', call_result.lower()):
                result = "не_определен"
            elif re.search(r'отказ', call_result.lower()) or re.search(r'ребенок', call_result.lower()) or \
                    re.search(r'уже пользуется тарифом', call_result.lower()) or\
                    re.search(r'больше не звонить', call_result.lower()) or re.search(r'пожилой человек', call_result.lower()):
                result = "отказ"
            elif re.search(r'автоответчик', call_result.lower()):
                result = "автоответчик"
            elif re.search(r'согласие', call_result.lower()):
                result = "согласие"
            else:
                result = "не_определен"
        else:
            result = "не_определен"
        return result


class DialogMigrator():

    def __init__(self, agent_uuid, bucket, date_from, date_to, project_alias):
        self.bucket = bucket
        self.date_from = date_from
        self.date_to = date_to
        self.agent_uuid = agent_uuid
        self.project_alias = project_alias
        self.unique_msisdns_count = None

    def copy_records_to_s3(self, path, record):
        try:
            object = neuro_s3_client.get_object(Bucket=self.bucket, Key=record)
        except Exception as e:
            logger.error(f'Problem with download object: {record}, from bucket {self.bucket} with error: {e}')
            return None, None, record
        try:
            content = object['Body'].read()
        except Exception as e:
            logger.info(f'Problem with getting object content from yandex s3, bucket: {record}, with error: {e}')
            exit(2)
        try:
            megafon_s3_client.put_object(Bucket='robots-neuro-records',
                                         Key=f'{path}', Body=content)
            return True, path, record

        except Exception as e:
            logger.error(f'Problem with uploading record: {path}, with error: {e}')
            return None, None, record

    def upload_records_with_treads(self, dialog_stat):
        data = dialog_stat.get('data')
        if data.get('uuid'):
            uuid = data.get('uuid')
        elif data.get('call_uuid'):
            uuid = data.get('call_uuid')
        else:
            match = re.search(r'(?<=(call_uuid=)).*(?=($))', data.get('call_record'))
            uuid = match[0]
        record_session = f'{uuid}.wav'
        msisdn = dialog_stat.get('msisdn')
        call_result = data.get("result")
        result = DialogRepositoryEntity.get_result(call_result)
        attempt = data.get("attempt")
        date = dialog_stat.get('date')
        try:
            date_format = datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
            date_name = date_format.date()
        except:
            date_format = date
            date_name = date.date()
        if re.search(r'(?:\+7|8)', msisdn):
            pass
        else:
            msisdn = '+7' + str(msisdn)
        month_year = f'{str(date_format.year)}_{str(date_format.month)}'
        file_name = f"{msisdn}_{result}_{str(attempt)}_{str(uuid)}.wav"
        folder = f"nn_{str(date_name)}_{self.project_alias}_{str(self.unique_msisdns_count)}"
        path = os.path.join(str(month_year), self.project_alias, folder, result, file_name)
        return self.copy_records_to_s3(path, record_session)

    def relocate_records(self):
        statistic_dialog_report = DialogRepositoryEntity.mongo_connect()
        dialogs_stat = statistic_dialog_report.find({'agent_uuid': self.agent_uuid, 'date':
                                        {'$gte': self.date_from, '$lte': self.date_to},
                                        "data.status": "+OK"})
        logger.info(f'from {self.date_from} to {self.date_to}, agent: {self.agent_uuid} dialog count:{dialogs_stat.count()}')

        if dialogs_stat.count() == 0:
            return None, None, None
        list_of_error_records = []
        file_error = result_path + f'{self.agent_uuid}_from:{self.date_from}_to:{self.date_to}_error.txt'
        self.unique_msisdns_count = len(dialogs_stat.distinct('msisdn'))
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for result, path, record in executor.map(self.upload_records_with_treads, dialogs_stat):
                if not result and record:
                    list_of_error_records.append(record)
                    continue
                logger.info(f'Record: {path} successful copied from {self.date_from} to {self.date_to},'
                            f' agent uuid: {self.agent_uuid}')
        if len(list_of_error_records) > 0:
            with open(file_error, 'w+') as error:
                for z in list_of_error_records:
                    error.write(z + '\n')
        return True


def main():
    repository_entity = DialogRepositoryEntity
    locker = repository_entity.pid_locker()
    for project in agents_space:
        for agent_uuid, project_alias in project.items():
            bucket = repository_entity.get_bucket(agent_uuid)
            date_from, date_to = repository_entity.get_dates()
            date_delta = (date_to - date_from).days
            date_generated = [date_from + datetime.timedelta(days=x) for x in range(1, date_delta)]
            date_generated.append(date_to)
            for date_to in date_generated:
                date_from = date_to - timedelta(days=1)

                statistic_object = DialogMigrator(agent_uuid, bucket, date_from, date_to, project_alias)
                statistic_object.relocate_records()

    logger.info(f'---------- DONE ----------')


if __name__ == '__main__':
    main()
