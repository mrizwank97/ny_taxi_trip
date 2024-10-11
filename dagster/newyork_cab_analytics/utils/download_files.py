import requests
def download_file(context, url, destination):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    context.log.info(f"Total size: {total_size}")
    block_size = 1024  # 1 Kibibyte
    with open(destination, 'wb') as file:
        for data in response.iter_content(block_size):
            file.write(data)
    context.log.info(f"Download complete: {destination}")