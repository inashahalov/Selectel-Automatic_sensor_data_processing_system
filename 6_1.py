import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
import os


class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str, ca_bundle: str = None):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint.strip(),  # Убираем лишние пробелы
            "region_name": "ru-7"  # Указываем регион, если нужно
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

    async def send_file(self, local_source: str):
        file_ref = Path(local_source)
        if not file_ref.is_file():
            raise ValueError(f"The path {file_ref} is not a file.")

        target_name = file_ref.name
        try:
            async with self._connect() as remote:
                with file_ref.open("rb") as binary_data:
                    await remote.put_object(
                        Bucket=self._bucket,
                        Key=target_name,
                        Body=binary_data
                    )
                    print(f"[+] Файл {target_name} успешно загружен")
        except ClientError as e:
            print(f"[-] Ошибка при загрузке файла: {e}")
        except FileNotFoundError as e:
            print(f"[-] Ошибка: {e}")

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
                    print(f"[+] Файл {remote_name} успешно сохранён как {local_target}")
        except ClientError as e:
            print(f"[-] Ошибка при скачивании файла: {e}")
        except FileNotFoundError as e:
            print(f"[-] Ошибка: {e}")

    async def remove_file(self, remote_name: str):
        try:
            async with self._connect() as remote:
                await remote.delete_object(Bucket=self._bucket, Key=remote_name)
                print(f"[+] Файл {remote_name} удалён")
        except ClientError as e:
            print(f"[-] Ошибка при удалении файла: {e}")
        except FileNotFoundError as e:
            print(f"[-] Ошибка: {e}")

    async def list_files(self):
        try:
            async with self._connect() as remote:
                response = await remote.list_objects_v2(Bucket=self._bucket)
                files = [content['Key'] for content in response.get('Contents', [])]
                return files
        except ClientError as e:
            print(f"[-] Ошибка при получении списка файлов: {e}")
            return []

    async def file_exists(self, remote_name: str):
        try:
            async with self._connect() as remote:
                await remote.head_object(Bucket=self._bucket, Key=remote_name)
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"[-] Ошибка при проверке наличия файла: {e}")
                return False


async def run_demo():
    ca_bundle_path = os.path.expanduser("~/.selectels3/root.crt")

    # Проверяем наличие файла сертификата
    if not os.path.isfile(ca_bundle_path):
        print(f"[-] Файл сертификата {ca_bundle_path} не найден. Скачиваем...")
        os.makedirs(os.path.dirname(ca_bundle_path), exist_ok=True)
        import urllib.request
        urllib.request.urlretrieve("https://s3.selcdn.ru/selcloud/selcloud-ca-chain.crt ", ca_bundle_path)
        print(f"[+] Файл сертификата {ca_bundle_path} успешно скачан.")

    storage = AsyncObjectStorage(
        key_id="ad151ebbbda048f2b817427893c1dc23",
        secret="a4f8929f51284d87968c2f375878398e",
        endpoint="https://s3.ru-7.storage.selcloud.ru ",  # Убираем лишние пробелы
        container="dataengineer-practice.s3.ru-7.storage.selcloud.ru",
        ca_bundle=ca_bundle_path
    )

    # Укажите путь к тестовому файлу
    test_file_path = "/home/f/Документы/Selectel/test.txt"
    download_dir = "/home/f/Документы/Selectel/downloads/"

    await storage.send_file(test_file_path)

    # Выводим список файлов в бакете
    files = await storage.list_files()
    print("[+] Список файлов в бакете:", files)

    # Проверяем наличие файла
    exists = await storage.file_exists("test.txt")
    print(f"[+] Файл 'test.txt' существует: {exists}")

    await storage.fetch_file("test.txt", download_dir + "downloaded_test.txt")
    await storage.remove_file("test.txt")

    # Проверяем наличие файла после удаления
    exists_after_removal = await storage.file_exists("test.txt")
    print(f"[+] Файл 'test.txt' существует после удаления: {exists_after_removal}")


if __name__ == "__main__":
    asyncio.run(run_demo())