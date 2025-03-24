import boto3
import paramiko.ed25519key
from conexion import ConectorAWS
import paramiko
import os
from botocore.exceptions import ClientError
import time


conector = ConectorAWS()
ec2 = conector.conectarse()
ec2_client = conector.conectarse_client()
efs = conector.conectarse_efs()
s3=conector.conectarse_s3()
athena=conector.conectarse_athena()

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
def montar_y_copiar_archivo(id_instancia, archivo_local, ruta_remota):
    try:
        # Conectar a la instancia EC2
        instancia = ec2.Instance(id_instancia)
        
        # Imprimir el estado actual de la instancia
        print(f'Estado actual de la instancia {id_instancia}: {instancia.state["Name"]}')
        
        # Esperar hasta que la instancia esté en ejecución
        if instancia.state["Name"] != "running":
            instancia.start()
            instancia.wait_until_running()
            print(f'La instancia {id_instancia} ha sido encendida.')
        else:
            print(f'La instancia {id_instancia} ya está en ejecución.')        
        
        # Actualizar la información de la instancia
        instancia.reload()
        ip_publica = instancia.public_ip_address
        print(f'La instancia {id_instancia} está en ejecución con IP pública {ip_publica}')
        
        # Asegúrate de que la ruta al archivo de clave privada sea correcta
        ruta_clave_privada = "claves.pem"
        
        # Verificar si el archivo de clave privada existe
        if not os.path.isfile(ruta_clave_privada):
            raise FileNotFoundError(f'El archivo de clave privada no se encontró en la ruta: {ruta_clave_privada}')
        
        key = paramiko.Ed25519Key.from_private_key_file(ruta_clave_privada)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip_publica, username="ec2-user", pkey=key)
        print('Conexión SSH establecida.')

        # Montar el volumen EBS
        comandos = [
            f"sudo mkdir {ruta_remota}", 
            f"sudo chmod 777 {ruta_remota}"  # Añadir permisos de escritura
        ]
        for comando in comandos:
            stdin, stdout, stderr = ssh.exec_command(comando)
            stdout.channel.recv_exit_status()
            print(f'Ejecutado comando: {comando}')
            print(f'Salida: {stdout.read().decode()}')
            print(f'Error: {stderr.read().decode()}')

        # Copiar el archivo al volumen montado
        sftp = ssh.open_sftp()
        print(f'Iniciando la transferencia del archivo {archivo_local} a {ruta_remota}')
        sftp.put(archivo_local, ruta_remota)
        sftp.close()
        ssh.close()
        print(f'Archivo {archivo_local} copiado a {ruta_remota} en la instancia {id_instancia}.')
    except Exception as e:
        print(f'Ocurrió un error: {e}')

# Función para crear un sistema de archivos EFS y montarlo
def crear_y_montar_efs(id_instancia, archivo_local, ruta_remota):
    try:
        # Crear el sistema de archivos EFS
        sistema_archivos = efs.create_file_system(
            PerformanceMode='generalPurpose'
        )
        file_system_id = sistema_archivos['FileSystemId']
        print(f'Sistema de archivos EFS {file_system_id} creado.')

        # Conectar a la instancia EC2
        instancia = ec2.Instance(id_instancia)
        
        # Imprimir el estado actual de la instancia
        print(f'Estado actual de la instancia {id_instancia}: {instancia.state["Name"]}')
        
        # Esperar hasta que la instancia esté en ejecución
        if instancia.state["Name"] != "running":
            instancia.start()
            instancia.wait_until_running()
            print(f'La instancia {id_instancia} ha sido encendida.')
        else:
            print(f'La instancia {id_instancia} ya está en ejecución.')        
        
        # Actualizar la información de la instancia
        instancia.reload()
        ip_publica = instancia.public_ip_address
        print(f'La instancia {id_instancia} está en ejecución con IP pública {ip_publica}')
        
        # Asegúrate de que la ruta al archivo de clave privada sea correcta
        ruta_clave_privada = "claves.pem"
        
        # Verificar si el archivo de clave privada existe
        if not os.path.isfile(ruta_clave_privada):
            raise FileNotFoundError(f'El archivo de clave privada no se encontró en la ruta: {ruta_clave_privada}')
        
        key = paramiko.Ed25519Key.from_private_key_file(ruta_clave_privada)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip_publica, username="ec2-user", pkey=key)
        print('Conexión SSH establecida.')

        # Montar el sistema de archivos EFS
        comandos = [
            f"sudo apt-get update",
            f"sudo apt-get install -y amazon-efs-utils",
            f"sudo mkdir -p {ruta_remota}",
            f"sudo chmod 777 {ruta_remota}",
            f"sudo mount -t efs {file_system_id}:/ {ruta_remota}"
        ]
        for comando in comandos:
            stdin, stdout, stderr = ssh.exec_command(comando)
            stdout.channel.recv_exit_status()
            print(f'Ejecutado comando: {comando}')
            print(f'Salida: {stdout.read().decode()}')
            print(f'Error: {stderr.read().decode()}')

        # Copiar el archivo al sistema de archivos EFS montado
        sftp = ssh.open_sftp()
        print(f'Iniciando la transferencia del archivo {archivo_local} a {ruta_remota}')
        sftp.put(archivo_local,ruta_remota+"/"+archivo_local)
        sftp.close()
        ssh.close()
        print(f'Archivo {archivo_local} copiado a {ruta_remota} en la instancia {id_instancia}.')
    except Exception as e:
        print(f'Ocurrió un error: {e}')

# Función para crear un bucket S3 y añadir objetos
def descargar_archivo_s3(nombre_bucket, nombre_archivo, ruta_destino):
    s3.download_file(nombre_bucket, nombre_archivo, ruta_destino)
    print(f'Archivo {nombre_archivo} descargado del bucket {nombre_bucket} a {ruta_destino}.')

def crear_bucket_s3(nombre_bucket, clase_almacenamiento='STANDARD'):
    for nombre_bucket in nombres_buckets:
        try:
            s3.create_bucket(Bucket=nombre_bucket)
            print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento {clase_almacenamiento}.')
            return nombre_bucket
        except ClientError as e:
            print(f'Error al crear el bucket {nombre_bucket}: {e}')

def anadir_csv_s3(nombre_bucket):
    # Añadir un archivo CSV al bucket
            s3.put_object(
                Bucket=nombre_bucket,
                Key='data/sample.csv',
                Body='id,name,age\n1,John Doe,30\n2,Jane Doe,25'
            )
            print(f'Archivo CSV añadido al bucket {nombre_bucket}.')

            descargar_archivo_s3(nombre_bucket, 'data/sample.csv', 'sample.csv')

# Función para crear un bucket S3 con clase de almacenamiento Standard-IA y añadir un objeto
def crear_s3_standard_ia(nombre_bucket):
    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='STANDARD_IA'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')

    descargar_archivo_s3(nombre_bucket, 'data/sample.txt', 'sample.txt')

# Función para crear un bucket S3 con clase de almacenamiento Intelligent-Tiering y añadir un objeto
def crear_s3_intelligent_tiering(nombre_bucket):
    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample2.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='INTELLIGENT_TIERING'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')

    descargar_archivo_s3(nombre_bucket, 'data/sample2.txt', 'sample2.txt')

def descargar_archivo_glacier(nombre_bucket, nombre_archivo, ruta_destino):
    # Restaurar el objeto desde Glacier
    s3.restore_object(
        Bucket=nombre_bucket,
        Key=nombre_archivo,
        RestoreRequest={
            'Days': 1,
            'GlacierJobParameters': {
                'Tier': 'Standard'
            }
        }
    )
     
    print(f'Objeto {nombre_bucket}/{nombre_archivo} restaurado desde Glacier. Espera a que la restauración se complete.')

    # Esperar a que la restauración se complete
   

    while True:
        response = s3.head_object(Bucket=nombre_bucket, Key=nombre_archivo)
        if 'Restore' in response and 'ongoing-request="false"' in response['Restore']:
            print(f'Objeto {nombre_bucket}/{nombre_archivo} restaurado y listo para descargar.')
            break
        print('Esperando a que la restauración se complete...')
        time.sleep(30)
        break


    # Descargar el archivo restaurado
    #s3.download_file(nombre_bucket, nombre_archivo, ruta_destino)
    print(f'Archivo {nombre_archivo} descargado del bucket {nombre_bucket} a {ruta_destino}.')


# Función para crear un bucket S3 con clase de almacenamiento Glacier y añadir un objeto
def crear_s3_glacier(nombre_bucket):
    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample3.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='GLACIER'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')
    descargar_archivo_glacier(nombre_bucket , 'data/sample3.txt', 'sample3.txt')

def crear_s3_glacier_deep_archive(nombre_bucket):
    s3.create_bucket(Bucket=nombre_bucket)
    print(f'Bucket S3 {nombre_bucket} creado con clase de almacenamiento Glacier Deep Archive.')

    # Añadir un objeto al bucket
    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample4.txt',
        Body='Este es un archivo de texto de muestra.',
        StorageClass='DEEP_ARCHIVE'
    )
    print(f'Objeto añadido al bucket {nombre_bucket}.')
    descargar_archivo_glacier(nombre_bucket , 'data/sample4.txt', 'sample4.txt')
    


def habilitar_versionado_s3(nombre_bucket):
    s3.put_bucket_versioning(
        Bucket=nombre_bucket,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )
    print(f'Versionado habilitado en el bucket {nombre_bucket}.')

    s3.put_object(
        Bucket=nombre_bucket,
        Key='data/sample.txt',
        Body='Este es un archivo de texto de muestra modificado.',
        StorageClass='STANDARD_IA'
    )
    response = s3.list_object_versions(Bucket=nombre_bucket, Prefix='data/sample.txt')
    for version in response.get('Versions', []):
        print(f'VersionId: {version["VersionId"]}, LastModified: {version["LastModified"]}, IsLatest: {version["IsLatest"]}')

def ejecutar_consulta(query):
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': 's3://prueba-bucket-67890/athena'}
    )
    return response['QueryExecutionId']

# Función para consultar un archivo CSV en S3 usando Athena
def crear_base_datos_athena(nombre_base_datos):

    ejecutar_consulta(f'CREATE DATABASE IF NOT EXISTS {nombre_base_datos}')
    ejecutar_consulta(f'CREATE EXTERNAL TABLE IF NOT EXISTS {nombre_base_datos}.usuarios (id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LOCATION "s3://prueba-bucket-67890/data/"')

    
    return nombre_base_datos
def esperar_resultado(query_execution_id):
    while True:
        estado = athena.get_query_execution(QueryExecutionId=query_execution_id)
        estado_actual = estado['QueryExecution']['Status']['State']
        if estado_actual in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return estado_actual
        time.sleep(2)

def consultar_s3_con_athena(base_datos, tabla, consulta):
    crear_base_datos_athena(base_datos)

    consultas = [
    f"SELECT * FROM {base_datos}.{tabla};",
    "SELECT COUNT(*) AS total_registros FROM mi_base_datos.sample_table;",
    "SELECT age, COUNT(*) AS cantidad FROM mi_base_datos.sample_table GROUP BY age;"
    ]

    for i, consulta in enumerate(consultas, start=1):
        print(f"\n⏳ Ejecutando consulta {i}...")
        query_id = ejecutar_consulta(consulta)
        estado = esperar_resultado(query_id)
        
        if estado == 'SUCCEEDED':
            resultado = athena.get_query_results(QueryExecutionId=query_id)
            print(f"Resultado consulta {i}:")
            # Mostrar resultados en texto
            for row in resultado['ResultSet']['Rows'][1:]:
                valores = [d.get('VarCharValue', '') for d in row['Data']]
                print(valores)
        else:
            print(f"La consulta {i} falló con estado: {estado}")



# Ejemplo de uso
if __name__ == "__main__":
    '''
    print('Crear una instancia EC2, ejecutarla, pararla y eliminarla ')
    gestionar_instancia_ec2()
    '''
    '''
    print('Cerar un EBS y asociarlo a un EC2 y añadir una archivo')
    #crear_y_adjuntar_ebs('i-063b41408d8e4402a')
    montar_y_copiar_archivo('i-063b41408d8e4402a', 'prueba.txt', '/home/ec2-user/archivos')
    
    print('Crear un EFS, montarlo y añadir un archivo')
    crear_y_montar_efs('i-063b41408d8e4402a', 'prueba.txt' , '/home/ec2-user/archivos2')
    '''

    print('Crear un S3 Estándar, crear un cubo y añadir varias carpetas con un objeto que sea un archivo csv con varios datos para trabajar con él a posteriori y obtener le objeto')
    nombres_buckets = [
        'prueba-bucket-12345',
        'prueba-bucket-67890',
        'prueba-bucket-abcdef',
        'prueba-bucket-ghijkl',
        'prueba-bucket-mnopqr',
        'prueba-bucket-stuvwx'
    ]
    '''
    anadir_csv_s3(crear_bucket_s3(nombres_buckets))

    print('Crear S3 Estándar - Acceso poco frecuente, crear un cubo y añadir un objeto y obtener le objeto')
    crear_s3_standard_ia(crear_bucket_s3(nombres_buckets, 'STANDARD_IA'))

    print('Crear S3 Intelligent-Tiering, crear un cubo y añadir un objeto y obtener le objeto')
    crear_s3_intelligent_tiering(crear_bucket_s3(nombres_buckets,'INTELLIGENT_TIERING'))
   

    print('Crear S3 Glacier, crear un cubo y añadir un objeto y obtener le objeto')
    crear_s3_glacier(crear_bucket_s3(nombres_buckets,'Glacier'))
    '''
    '''
    print('Crear S3 Glacier Deep Archive, crear un cubo y añadir un objeto y obtener le objeto ')
    crear_s3_glacier_deep_archive(crear_bucket_s3(nombres_buckets,'Deep_Archive'))
    '''

    '''
    print('Hablitar el control de versiones de S3 mediante comandos y mostrar un ejemplo de un objeto modificado y mostrar dos versiones')
    habilitar_versionado_s3('prueba-bucket-67890')'
    '''

    print('Realizar 3 consultas diferentes sobre el objeto .csv del S3 usando AWS Athena')
    consultar_s3_con_athena(crear_base_datos_athena('Trabajo_nube'), 'usuarios', 'SELECT * FROM sample.csv LIMIT 10;')
    
    

    
