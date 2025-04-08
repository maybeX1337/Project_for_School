import asyncio
from contextlib import asynccontextmanager
from typing import List

import aiofiles
from aiobotocore.session import get_session
from botocore.exceptions import ClientError


class S3Client:
    def __init__(
            self,
            access_key: str,
            secret_key: str,
            endpoint_url: str,
            bucket_name: str,
    ):
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url,
        }
        self.bucket_name = bucket_name
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        async with self.session.create_client("s3", **self.config) as client:
            yield client

    async def upload_file(
            self,
            file_path: str,
    ):
        object_name = file_path.split("/")[-1]  # /users/artem/cat.jpg
        try:
            async with self.get_client() as client:
                with open(file_path, "rb") as file:
                    await client.put_object(
                        Bucket=self.bucket_name,
                        Key=object_name,
                        Body=file,
                    )
                print(f"File {object_name} uploaded to {self.bucket_name}")
        except ClientError as e:
            print(f"Error uploading file: {e}")

    async def delete_file(self, object_name: str):
        try:
            async with self.get_client() as client:
                await client.delete_object(Bucket=self.bucket_name, Key=object_name)
                print(f"File {object_name} deleted from {self.bucket_name}")
        except ClientError as e:
            print(f"Error deleting file: {e}")

    async def get_file(self, object_name: str, destination_path: str):
        try:
            async with self.get_client() as client:
                response = await client.get_object(Bucket=self.bucket_name, Key=object_name)
                data = await response["Body"].read()
                with open(destination_path, "wb") as file:
                    file.write(data)
                print(f"File {object_name} downloaded to {destination_path}")
        except ClientError as e:
            print(f"Error downloading file: {e}")



async def save_to_file(data: List[str], filename: str = "responses.txt"):
    async with aiofiles.open(filename, 'w') as f:
        for line in data:
            await f.write(line + '\n')
    print(f"Saved responses to {filename}")

async def add_to_s3(response_text, pipeline_name):
    s3_client = S3Client(
        access_key="cc51416",
        secret_key="1ed4aa05c4ba260e6a7e10c321baca74",
        endpoint_url="https://s3.timeweb.com",  # для Selectel используйте https://s3.storage.selcloud.ru
        bucket_name="20fa9cdd-0a1061b9-a060-4397-984c-9d3956a29b77",
    )
    async with aiofiles.open(f'./{pipeline_name}.txt', mode='a') as file:
            await file.write(f'{response_text}\n')
    # Проверка, что мы можем загрузить, скачать и удалить файл

    await s3_client.upload_file(f"./{pipeline_name}.txt")
    await s3_client.get_file(f"{pipeline_name}.txt", f"./{pipeline_name}_read.txt")


