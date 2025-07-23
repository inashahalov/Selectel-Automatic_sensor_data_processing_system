import asyncio
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
import pandas as pd
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import tempfile

# Настройка логирования
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str, ca_bundle: str = None):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint.strip(),
            "region_name": "ru-7"
        }
        self._bucket = container
        self._session = get_session()
        self._ca_bundle = ca_bundle

        if self._ca_bundle and not os.path.isfile(self._ca_bundle):
            raise FileNotFoundError(f"The CA bundle file {self._ca_bundle} does not exist.")

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth, verify=self._ca_bundle) as connection:
            yield connection

    async def send_file(self, local_source: str, remote_name: str):
        file_ref = Path(local_source)
        if not file_ref.is_file():
            raise ValueError(f"The path {file_ref} is not a file.")

        try:
            async with self._connect() as remote:
                with file_ref.open("rb") as binary_:
                    await remote.put_object(
                        Bucket=self._bucket,
                        Key=remote_name,
                        Body=binary_
                    )
                    logging.info(f"[+] Файл {remote_name} успешно загружен")
        except ClientError as e:
            logging.error(f"[-] Ошибка при загрузке файла: {e}")
        except FileNotFoundError as e:
            logging.error(f"[-] Ошибка: {e}")

    async def fetch_file(self, remote_name: str, local_target: str):
        try:
            async with self._connect() as remote:
                response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
                body = await response["Body"].read()
                local_target_path = Path(local_target)
                if not local_target_path.parent.exists():
                    local_target_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_target, "wb") as out:
                    out.write(body)
                    logging.info(f"[+] Файл {remote_name} успешно сохранён как {local_target}")
        except ClientError as e:
            logging.error(f"[-] Ошибка при скачивании файла: {e}")
        except FileNotFoundError as e:
            logging.error(f"[-] Ошибка: {e}")

    async def remove_file(self, remote_name: str):
        try:
            async with self._connect() as remote:
                await remote.delete_object(Bucket=self._bucket, Key=remote_name)
                logging.info(f"[+] Файл {remote_name} удалён")
        except ClientError as e:
            logging.error(f"[-] Ошибка при удалении файла: {e}")
        except FileNotFoundError as e:
            logging.error(f"[-] Ошибка: {e}")

class CSVHandler(FileSystemEventHandler):
    def __init__(self, storage: AsyncObjectStorage, source_dir: str, archive_dir: str, temp_dir: str, loop: asyncio.AbstractEventLoop):
        self.storage = storage
        self.source_dir = Path(source_dir)
        self.archive_dir = Path(archive_dir)
        self.temp_dir = Path(temp_dir)
        self.loop = loop
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.csv':
            asyncio.run_coroutine_threadsafe(self.process_csv(file_path), self.loop)

    async def process_csv(self, file_path: Path):
        logging.info(f"[+] Новый файл: {file_path}")
        try:
            # Чтение CSV-файла
            df = pd.read_csv(file_path)
            logging.info(f"[+] Файл {file_path} прочитан")

            # Пример фильтрации: удаляем строки с NaN значениями
            df_filtered = df.dropna()
            logging.info(f"[+] Файл {file_path} отфильтрован")

            # Сохранение отфильтрованного DataFrame во временный файл
            temp_file_path = self.temp_dir / file_path.name
            df_filtered.to_csv(temp_file_path, index=False)
            logging.info(f"[+] Файл {file_path} сохранен во временный файл {temp_file_path}")

            # Загрузка файла в S3
            remote_name = f"processed/{file_path.name}"
            await self.storage.send_file(temp_file_path, remote_name)
            logging.info(f"[+] Файл {temp_file_path} загружен в S3 как {remote_name}")

            # Перемещение оригинального файла в архив
            archive_path = self.archive_dir / file_path.name
            shutil.move(str(file_path), str(archive_path))
            logging.info(f"[+] Оригинальный файл {file_path} перемещен в архив {archive_path}")

            # Удаление временного файла
            temp_file_path.unlink()
            logging.info(f"[+] Временный файл {temp_file_path} удален")

        except Exception as e:
            logging.error(f"[-] Ошибка при обработке файла {file_path}: {e}")

async def main():
    ca_bundle_path = os.path.expanduser("~/.selectels3/root.crt")

    # Проверяем наличие файла сертификата
    if not os.path.isfile(ca_bundle_path):
        logging.error(f"[-] Файл сертификата {ca_bundle_path} не найден. Скачиваем...")
        os.makedirs(os.path.dirname(ca_bundle_path), exist_ok=True)
        import urllib.request
        urllib.request.urlretrieve("https://s3.selcdn.ru/selcloud/selcloud-ca-chain.crt ", ca_bundle_path)
        logging.info(f"[+] Файл сертификата {ca_bundle_path} успешно скачан.")

    storage = AsyncObjectStorage(
        key_id="ad151ebbbda048f2b817427893c1dc23",
        secret="a4f8929f51284d87968c2f375878398e",
        endpoint="https://s3.ru-7.storage.selcloud.ru ",
        container="dataengineer-practice.s3.ru-7.storage.selcloud.ru",
        ca_bundle=ca_bundle_path
    )

    source_dir = "/home/f/Документы/Selectel/source"
    archive_dir = "/home/f/Документы/Selectel/archive"
    temp_dir = "/home/f/Документы/Selectel/temp"

    loop = asyncio.get_event_loop()
    event_handler = CSVHandler(storage, source_dir, archive_dir, temp_dir, loop)
    observer = Observer()
    observer.schedule(event_handler, path=source_dir, recursive=False)
    observer.start()
    logging.info("[+] Начало отслеживания папки")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("[+] Отслеживание остановлено")

    observer.join()
    logging.info("[+] Завершение работы")

if __name__ == "__main__":
    asyncio.run(main())