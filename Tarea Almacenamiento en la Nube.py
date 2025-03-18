import boto3
from conexion import ConectorAWS
import paramiko



conector = ConectorAWS()
ec2 = conector.conectarse()
ec2_client = conector.conectarse_client()

# Función para crear, iniciar, detener y terminar una instancia EC2
def gestionar_instancia_ec2():
    # Crear una instancia EC2
    instancia = ec2.create_instances(
        ImageId='ami-08b5b3a93ed654d19', 
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro',
        KeyName='claves'  
    )[0]
    instancia.wait_until_running()
    print(f'Instancia EC2 {instancia.id} creada y en ejecución.')

    # Detener la instancia
    instancia.stop()
    instancia.wait_until_stopped()
    print(f'Instancia EC2 {instancia.id} detenida.')

    # Iniciar la instancia
    instancia.start()
    instancia.wait_until_running()
    print(f'Instancia EC2 {instancia.id} iniciada.')

    # Terminar la instancia
    instancia.terminate()
    instancia.wait_until_terminated()
    print(f'Instancia EC2 {instancia.id} terminada.')

# Función para crear un volumen EBS y adjuntarlo a una instancia EC2
def crear_y_adjuntar_ebs(id_instancia):
    volumen = ec2_client.create_volume(
        AvailabilityZone='us-east-1d', 
        Size=10,  # Tamaño en GiB
        VolumeType='gp3'
    )
    ec2_client.get_waiter('volume_available').wait(VolumeIds=[volumen['VolumeId']])
    ec2_client.attach_volume(
        VolumeId=volumen['VolumeId'],
        InstanceId=id_instancia,
        Device='/dev/sdh'
    )
    print(f'Volumen EBS {volumen["VolumeId"]} creado y adjuntado a la instancia {id_instancia}.')
    return volumen['VolumeId']

# Función para montar el volumen EBS y copiar un archivo
def montar_y_copiar_archivo(id_instancia, volume_id, archivo_local, ruta_remota):
    # Conectar a la instancia EC2
    instancia = ec2.Instance(id_instancia)
    instancia.wait_until_running()
    ip_publica = instancia.public_ip_address

    key = paramiko.RSAKey.from_private_key_file("ruta/a/tu/clave.pem")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip_publica, username="ec2-user", pkey=key)

    # Montar el volumen EBS
    comandos = [
        f"sudo mkfs -t ext4 /dev/{volume_id}",  # Cambiar a /dev/sdf
        "sudo mkdir /mnt/ebs",
        f"sudo mount /dev/{volume_id} /mnt/ebs"  # Cambiar a /dev/sdf
    ]
    for comando in comandos:
        stdin, stdout, stderr = ssh.exec_command(comando)
        stdout.channel.recv_exit_status()

    # Copiar el archivo al volumen montado
    sftp = ssh.open_sftp()
    sftp.put(archivo_local, ruta_remota)
    sftp.close()
    ssh.close()
    print(f'Archivo {archivo_local} copiado a {ruta_remota} en la instancia {id_instancia}.')


# Función para crear un sistema de archivos EFS y montarlo
def crear_y_montar_efs():
    efs = boto3.client('efs')
    sistema_archivos = efs.create_file_system(
        PerformanceMode='generalPurpose'
    )
    print(f'Sistema de archivos EFS {sistema_archivos["FileSystemId"]} creado.')

# Función para crear un bucket S3 y añadir objetos
def crear_bucket_s3(nombre_bucket, clase_almacenamiento='STANDARD'):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=nombre_bucket)
    print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento {clase_almacenamiento}.')

    # Añadir un archivo CSV al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.csv',
        Body='id,name,age\n1,John Doe,30\n2,Jane Doe,25'
    )
    print(f'Archivo CSV añadido al bucket {nombre_bucket}.')

# Función para habilitar el versionado en un bucket S3
def habilitar_versionado_s3(nombre_bucket):
    s3 = boto3.client('s3')
    s3.put_bucket_versioning(
        Bucket=nombre_bucket,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )
    print(f'Versionado habilitado en el bucket {nombre_bucket}.')

# Función para consultar un archivo CSV en S3 usando Athena
def consultar_s3_con_athena(base_datos, tabla, consulta):
    athena = boto3.client('athena')
    respuesta = athena.start_query_execution(
        QueryString=consulta,
        QueryExecutionContext={
            'Database': base_datos
        },
        ResultConfiguration={
            'OutputLocation': 's3://your-query-results-bucket/'  # Reemplazar con tu bucket S3 para resultados de consultas
        }
    )
    print(f'Consulta Athena iniciada: {respuesta["QueryExecutionId"]}')

# Función para crear un bucket S3 con clase de almacenamiento Standard-IA y añadir un objeto
def crear_s3_standard_ia(nombre_bucket):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=nombre_bucket)
    print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento Standard-IA.')

    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='STANDARD_IA'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')

# Función para crear un bucket S3 con clase de almacenamiento Intelligent-Tiering y añadir un objeto
def crear_s3_intelligent_tiering(nombre_bucket):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=nombre_bucket)
    print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento Intelligent-Tiering.')

    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='INTELLIGENT_TIERING'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')

# Función para crear un bucket S3 con clase de almacenamiento Glacier y añadir un objeto
def crear_s3_glacier(nombre_bucket):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=nombre_bucket)
    print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento Glacier.')

    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='GLACIER'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')

# Función para crear un bucket S3 con clase de almacenamiento Glacier Deep Archive y añadir un objeto
def crear_s3_glacier_deep_archive(nombre_bucket):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=nombre_bucket)
    print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento Glacier Deep Archive.')

    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='DEEP_ARCHIVE'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')

# Ejemplo de uso
if __name__ == "__main__":
    '''
    print('Crear una instancia EC2, ejecutarla, pararla y eliminarla ')
    gestionar_instancia_ec2()
    '''

    print('Cerar un EBS y asociarlo a un EC2 y añadir una archivo')
    crear_y_adjuntar_ebs('i-063b41408d8e4402a')
    volume_id = crear_y_adjuntar_ebs('i-063b41408d8e4402a')
    montar_y_copiar_archivo(id_instancia, volume_id, 'ruta/a/tu/archivo.txt', '/mnt/ebs/archivo.txt')

    '''
    crear_y_montar_efs()
    crear_bucket_s3('your-standard-bucket')
    habilitar_versionado_s3('your-versioned-bucket')
    consultar_s3_con_athena('your-database', 'your-table', 'SELECT * FROM your-table LIMIT 10;')
    crear_s3_standard_ia('your-standard-ia-bucket')
    crear_s3_intelligent_tiering('your-intelligent-tiering-bucket')
    crear_s3_glacier('your-glacier-bucket')
    crear_s3_glacier_deep_archive('your-glacier-deep-archive-bucket')'
    '''
