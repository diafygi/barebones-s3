# Python barebones AWS S3 API utility

This is a small Python 3 function that you can use in your projects to manage files in AWS S3.

It has no other dependencies besides python's standard library, so you can copy-paste `barebones_s3.py` into your project and import it, or you can copy-paste the function code directly into your codebase.

I made this as a small helper utility for my personal projects that need to save files to S3.

## Example use

```python
from html import escape
from xml.etree.ElementTree import fromstring
from barebones_s3 import s3_request

AWS_CONF = {
    "bucket": "mybucket",
    "aws_region": "us-east-1",
    "aws_key_id": "...",
    "aws_secret": "...",
}

# upload a file
resp1 = s3_request("PUT", "/test.txt", headers={"Content-Type": "text/plain"}, body=b"My content.", **AWS_CONF)

# retrieve a file
resp2 = s3_request("GET", "/test.txt", **AWS_CONF)

# get the headers for a file
resp3 = s3_request("HEAD", "/test.txt", **AWS_CONF)
print(resp3.headers)

# list all files with a prefix filter
query = {"list-type": "2", "prefix": "te"}
s3_obj_list = []
while True:
    resp4 = s3_request("GET", "/", query=query, **AWS_CONF)
    resp4_xml = fromstring(resp4.read().decode())
    for contents in resp4_xml.findall("{*}Contents"):
        s3_obj_list.append({
            "Key": contents.find("{*}Key").text,
            "LastModified": contents.find("{*}LastModified").text,
            "ETag": contents.find("{*}ETag").text,
            "Size": int(contents.find("{*}Size").text),
            "StorageClass": contents.find("{*}StorageClass").text,
        })
        print(s3_obj_list[-1]['Key'])
    continuationToken = resp4_xml.find("{*}NextContinuationToken")
    if continuationToken is not None:
        query['continuation-token'] = continuationToken.text
    else:
        break

# delete a file
resp5 = s3_request("DELETE", "/test.txt", **AWS_CONF)
print(resp5.status == 204)

# multipart upload (2x 6MB parts)
body1, body2 = b"a" * 6000000, b"b" * 6000000
resp6 = s3_request("POST", "/testmulti.txt", query={"uploads": ""}, **AWS_CONF)
upload_id = fromstring(resp6.read().decode()).find("{*}UploadId").text
resp7 = s3_request("PUT", "/testmulti.txt", query={"partNumber": 1, "uploadId": upload_id}, body=body1, **AWS_CONF)
etag1 = escape(resp7.headers['ETag'])
resp8 = s3_request("PUT", "/testmulti.txt", query={"partNumber": 2, "uploadId": upload_id}, body=body2, **AWS_CONF)
etag2 = escape(resp8.headers['ETag'])
completed_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Part><ETag>{etag1}</ETag><PartNumber>1</PartNumber></Part>
   <Part><ETag>{etag2}</ETag><PartNumber>2</PartNumber></Part>
</CompleteMultipartUpload>""".encode()
resp9 = s3_request("POST", "/testmulti.txt", query={"uploadId": upload_id}, body=completed_body, **AWS_CONF)
resp9_body = resp9.read() # multipart is complete upon full response (not just when status code is received)

# test that the file matches the parts
resp10 = s3_request("GET", "/testmulti.txt", **AWS_CONF)
resp10_body = resp10.read()
bool((body1 + body2) == resp10_body)
```

## Copyright

Released under the MIT License.
