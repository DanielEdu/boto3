import logging
import boto3
from io import BytesIO
import zipfile

"""
----------------------
"""

bucket_name = 'bucket'    # bucketname
prefix = 'prefix/' # prefix/
substring = 'string'      # String to search
f = open('/Users/user/Rappi/s3_key.txt','w+')
out_f = open('/Users/user/Rappi/s3_header.txt','w+')
s3_resource = boto3.resource('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket(name=bucket_name)
FilesNotFound = True


def unzip_s3(zip_key):
    
    zip_obj = s3_resource.Object(bucket_name=bucket_name, key=zip_key)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)

    for filename in z.namelist():
        file_info = z.getinfo(filename)
        
        with z.open(filename) as f:
            for line in f:
                decode = line.decode('utf8')
                if  substring in decode:
                    out_key = '{0}|{1}|{2}|{3}\n'.format(decode,zip_key,file_info.filename,file_info.file_size)
                    out_f.write(out_key+'\n')
                    print(out_key)
                    break
                else:
                    break




def run(prefix):
    for obj in bucket.objects.filter(Prefix=prefix):
        key = '{0}'.format(obj.key)
        f.write(key+'\n')       # Guardar nombre y ruta del archivo
        unzip_s3(key)




if __name__ == '__main__':
    # save all keys path in local
    run(prefix)
    f.close()
    out_f.close()
