from minio import Minio
from minio.error import S3Error
import io
import csv

# Dados do Minio
minio_host = "minio.ifpbapps.online"
minio_port = 443
minio_access_key = "VQdTNNVXZZLXNMRnV4RDFoQ2MtZGFSRjN3Q0Z2Z"
minio_secret_key = "pMnQ4ZkktaU9UdlRYX1c!3S1lHMkVpajU5LTRLTHl1NwbmRDVmdiWW9RaGJvZmFWSkF5YmZ"
minio_bucket = "raios"
minio_tratados_bucket = "raios-tratados"

# Função para conectar ao Minio
def connect_to_minio():
    return Minio(
        minio_host,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True,
    )


# Função para processar dados diretamente no Minio
def process_data(minio_client, bucket_name, object_name):
    try:
        # Baixar o arquivo
        response = minio_client.get_object(bucket_name, object_name)
        data = response.read().decode("latin-1")

        # Processar os dados
        treated_data = [line + " - Tratado" for line in data.split("\n")]

        # Upload do arquivo tratado para o bucket 'raios-tratados'
        minio_client.put_object(
            minio_tratados_bucket,
            object_name,
            io.BytesIO("\n".join(treated_data).encode()),
            len("\n".join(treated_data)),
        )
        print(f"Arquivo {object_name} tratado enviado para o Minio com sucesso.")
    except S3Error as e:
        print(f"Erro ao processar o arquivo {object_name} no Minio: {e}")

# Conectar ao Minio
minio_client = connect_to_minio()

# Listar objetos no bucket 'raios'
objects = minio_client.list_objects(minio_bucket)
for obj in objects:
    # Processar os dados diretamente no Minio
    process_data(minio_client, minio_bucket, obj.object_name)
