import io
import boto3
from PIL import Image
import logging
logger = logging.getLogger()
# access_key = 'DgEJlJmCpUpELcRyAj9F'
# access_token = 'axcixs48W3YsXxCNmCaYSspUEOHzkXJW0u0b7gmi'
bucket = 'phenotyping'

def split_s3_key(s3_key):
    key = str(s3_key)
    last_name = key.split('/')[-1]
    return key.replace(last_name, ""), last_name


def is_blank(str):
    if str and str.strip():
        return False
    return True

class s3_loader:
    def __init__(self,bucket):
        service_name = 's3'
        endpoint_url = 'https://kr.object.ncloudstorage.com'
        region_name = 'kr-standard'
        _access_key = 'DgEJlJmCpUpELcRyAj9F'
        _access_token = 'axcixs48W3YsXxCNmCaYSspUEOHzkXJW0u0b7gmi'
        self.bucket = bucket

        s3 = boto3.Session(region_name=region_name,
                           aws_access_key_id=_access_key,
                           aws_secret_access_key=_access_token)
        s3_down = s3.resource(service_name, endpoint_url=endpoint_url)  #
        self.s3bucket = s3_down.Bucket(bucket)


    def s3_list(self,s3_prefix, pattern=None, after_ts=0):
        objects = self.s3bucket.objects.filter(Prefix=s3_prefix)
        filenames = []
        count = 0
        for obj in objects:
            count += 1
            if pattern != None and not pattern in obj.key:
                continue
    
            last_modified_dt = obj.last_modified
            s3_ts = last_modified_dt.timestamp() * 1000
            if s3_ts > after_ts:
                s3_path, s3_filename = split_s3_key(obj.key)
                # directory check
                if is_blank(s3_filename) or s3_filename.endswith("/"):
                    logger.debug(f"{self.bucket} - {s3_path}{s3_filename} {last_modified_dt} is directory")
                else:
                    logger.debug(f"append {self.bucket} - {s3_path}{s3_filename} {last_modified_dt}")
                    filenames.append(s3_path+s3_filename)
                # filenames.append(s3_path + s3_filename)
    
        logger.debug(f"Total s3 bucket objects size = {count}")
        logger.debug(f"Final s3 bucket file list len = {len(filenames)}")
        return filenames
    
    def read_files(self, file_names):
        file = io.BytesIO()
        obj = self.s3bucket.Object(file_names)
        obj.download_fileobj(file)
        return file

    def upload_file(self,file_object,file_name):
        self.s3bucket.put_object(Body=file_object,
                            Key=file_name,
                            ACL='public-read')

if __name__ == '__main__':
    name = 'bean/210617/SIDE/RDA_SOYBEAN_B_01_15_VIS_SV198_2021-06-17.png'
    loader = s3_loader(bucket)
    file_names = loader.s3_list('','original')
    # file_list = get_object_list_name(bucket_name=bucket,directory_path='bean/',extension='png' )
    # img = Image.open(file)
    # img.show()
