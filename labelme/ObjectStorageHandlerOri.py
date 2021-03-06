from typing import List, Any

import boto3
import os

service_name = 's3'
endpoint_url = 'https://kr.object.ncloudstorage.com'
region_name = 'kr-standard'
access_key = 'DgEJlJmCpUpELcRyAj9F'
access_token = 'axcixs48W3YsXxCNmCaYSspUEOHzkXJW0u0b7gmi'

s3 = boto3.client(service_name, aws_access_key_id=access_key, aws_secret_access_key=access_token,
                  endpoint_url=endpoint_url)


# 버킷 목록 가져오기
def get_bucket_list():
    return s3.list_buckets()


# 버킷 내에 오브젝트 목록 가져오기
def get_object_list(bucket_name, max_key=300):
    object_response = s3.list_objects(Bucket=bucket_name, MaxKeys=max_key)
    return object_response.get('Contents')


# 디렉토리 내에 오브젝트 목록 가져오기 (확장자 지정)
def get_object_list_directory(bucket_name: str, directory_path: str, max_key: int = 300, extension: object = None):
    # 확장자가 지정이 안되었을 경우 기본 확장자 설정
    if extension is None:
        extension = []

    object_response = s3.list_objects(Bucket=bucket_name, MaxKeys=max_key)
    contents = object_response.get('Contents')

    # 디렉토리내 오브젝트를 담을 객체 생성
    items: list[Any] = []
    # 현재 디렉토리명 할당
    current_directory: str = ''
    # 현재 디렉토리 내의 서브디렉토리 담을 객체 생성
    sub_directory: list[Any] = []

    for item in contents:
        file_name = item.get('Key')
        if directory_path not in file_name:
            pass
        else:
            current_directory = directory_path
            delete_directory_path = item.get('Key').replace(directory_path + '/', '')

            if item.get('Size') == 0: # 서브 디렉토리 할당
                item['DirectoryName'] = delete_directory_path.rsplit('/')[0]
                if len(item['DirectoryName']) > 0:
                    sub_directory.append(item)
            else: # 오브젝트 객체 할당
                path_segments = delete_directory_path.rsplit('/')
                # 현재 디렉토리에 오브젝트인지 체크
                if len(path_segments) > 1:
                    continue
                # 지정된 확장명의 파일인지 체크
                if len(extension) != 0 and item.get('Key').rsplit('.')[1] not in extension:
                    continue

                items.append(item)

    return {
        'directory': current_directory,
        'subdirectory': sub_directory,
        'items': items
    }


# 오브젝트 다운로드
def download_object(bucket_name, object_name, save_path):
    s3.download_file(bucket_name, object_name, save_path)


# 디렉토리 다운로드
def download_directory(bucket_name, directory_name, save_path):
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    objectResponse = get_object_list_directory(bucket_name=bucket_name, directory_path=directory_name)

    items = objectResponse.get('items')
    for item in items:
        item_save_path = save_path + '/' + str(item.get('Key').rsplit('/')[-1])
        print(item_save_path)
        download_object(bucket_name=bucket_name, object_name=item.get('Key'), save_path=item_save_path)


# 오브젝트 업로드
def upload_object(bucket_name, local_file_path, directory):
    # 디렉토리 생성(디렉토리가 존재하지 않으면 생성)
    s3.put_object(Bucket=bucket_name, Key=directory)
    # 업로드할 오브젝트명 설정
    object_name = directory + '/' + local_file_path.rsplit('/')[-1]
    # 파일 업로드
    s3.upload_file(local_file_path, bucket_name, object_name)


# 디렉토리 업로드
def upload_directory(bucket_name, local_folder_path, directory):
    try:
        # 업로드할 디렉토리 설정
        upload_directory = directory + '/' + local_folder_path.rsplit('/')[-1]
        # 업로드할 파일목록
        filenames = os.listdir(local_folder_path)

        for filename in filenames:
            # 업도르할 파일의 경로 설정
            full_filename = os.path.join(local_folder_path, filename)
            # 파일 업로드
            upload_object(bucket_name, full_filename, upload_directory)
    except FileNotFoundError as NFE:
        print(NFE)
    except Exception as E:
        print(E)


if __name__ == '__main__':
    download_directory('ai-object-storage', 'labelme/download/01062537326', "C:/Users/admin/Document/labelme")
"""
    # 버킷 목록 가져오기
    response = get_bucket_list()
    print(response)

    upload_directory(bucket_name='ai-object-storage',
                     local_folder_path='/Users/hoseobkim/Documents/work/EchossTech/test', directory='upload_test')

    # downloadDirectory(bucket_name='ai-object-storage', directory_name=directory, save_path='/Users/hoseobkim/Documents/work/EchossTech/test')


    # objectResponse = get_object_list_directory(bucket_name='ai-object-storage', max_key=max_key,
    #                                            directory_path=directory, extension=exts)
    #
    # print(objectResponse)
    #
    # downloadObject('ai-object-storage', 'shrimp/2021-07-28/tomato_g_test2.jpg', '/Users/hoseobkim/Documents/work/EchossTech/tomato_g_test2.jpg')
"""