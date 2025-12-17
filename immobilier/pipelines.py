import json
import boto3
from datetime import datetime
import os

class S3UploadPipeline:

    def open_spider(self, spider):
        os.makedirs("data", exist_ok=True)

        self.filename = f"data/books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.file = open(self.filename, "w", encoding="utf-8")
        self.items = []

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        json.dump(self.items, self.file, ensure_ascii=False, indent=2)
        self.file.close()

        s3 = boto3.client("s3")
        bucket_name = "m2dsia-gueye-adama"
        s3_key = os.path.basename(self.filename)

        s3.upload_file(self.filename, bucket_name, s3_key)
        print(f"Uploaded {s3_key} to S3")
