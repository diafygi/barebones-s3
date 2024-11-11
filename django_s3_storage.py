from html import escape
from xml.etree.ElementTree import fromstring
from django.core.files.base import File
from django.core.files.storage.base import Storage
from django.conf import settings
from .barebones_s3 import s3_request, s3_open


class S3Storage(Storage):
    """
    This is a storage backend for Django that uses barebones_s3.

    Example settings.py:
    STORAGES = {
        ...
        "mystorage": {
            "BACKEND": "path.to.django_storage.S3Storage",
            "OPTIONS": {
                # NOTE: These options can be strings or callables
                "bucket": "mybucket",
                "aws_region": "us-east-1",
                "aws_key_id": "aaaaaaaaaaaaaaaaaa",
                "aws_secret": "bbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "session_token": None, # optional, used with temporary credentials
            }
        },
        ...
    }
    """

    do_multipart_at = 10000000  # 10MB
    multipart_chunk_size = 10000000  # 10MB

    def __init__(self, **kwargs):
        self.aws_conf = {
            "bucket": kwargs.pop("bucket"),
            "aws_region": kwargs.pop("aws_region"),
            "aws_key_id": kwargs.pop("aws_key_id", None),
            "aws_secret": kwargs.pop("aws_secret", None),
            "session_token": kwargs.pop("session_token", None),
        }
        self.do_multipart_at = kwargs.pop("do_multipart_at", self.do_multipart_at)
        self.multipart_chunk_size = kwargs.pop(
            "multipart_chunk_size", self.multipart_chunk_size
        )
        return super().__init__(**kwargs)

    def _open(self, name, mode="rb"):
        return File(s3_open(f"/{name}", mode, **self.aws_conf))

    def _save(self, name, content):
        save_name = self.get_available_name(name)
        # single-request upload for smaller files
        if content.size < self.do_multipart_at:
            resp = s3_request("PUT", f"/{save_name}", body=content, **self.aws_conf)
            assert resp.status == 200
        # mulitpart upload for larger files
        else:
            start_resp = s3_request(
                "POST", f"/{save_name}", query={"uploads": ""}, **self.aws_conf
            )
            assert start_resp.status == 200
            upload_id = fromstring(start_resp.read().decode()).find("{*}UploadId").text
            cur_part_id = 1
            part_entries = []
            while True:
                part_body = content.read(self.multipart_chunk_size)
                if not part_body:
                    break
                query = {"partNumber": cur_part_id, "uploadId": upload_id}
                part_resp = s3_request(
                    "PUT", f"/{save_name}", query=query, body=part_body, **self.aws_conf
                )
                assert part_resp.status == 200
                etag = escape(part_resp.headers["ETag"])
                part_entries.append(
                    f"<Part><ETag>{etag}</ETag><PartNumber>{cur_part_id}</PartNumber></Part>"
                )
                cur_part_id += 1
            end_resp_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                {''.join(part_entries)}</CompleteMultipartUpload>""".encode()
            end_resp = s3_request(
                "POST",
                f"/{save_name}",
                query={"uploadId": upload_id},
                body=end_resp_payload,
                **self.aws_conf,
            )
            # successful not when 200, but when there's a Location in the response body
            assert fromstring(end_resp.read().decode()).find("{*}Location") is not None
        return save_name

    def path(self, name):
        return name

    def delete(self, name):
        resp = s3_request("DELETE", f"/{name}", **self.aws_conf)
        assert resp.status == 204

    def exists(self, name):
        resp = s3_request("HEAD", f"/{name}", **self.aws_conf)
        assert resp.status in [200, 404]
        return resp.status == 200

    def listdir(self, path):
        directories, files = [], []
        path += "/" if path and not path.endswith("/") else ""
        query = {"list-type": "2", "prefix": path, "delimiter": "/"}
        while True:
            resp = s3_request("GET", "/", query=query, **self.aws_conf)
            assert resp.status == 200
            resp_bytes = resp.read().decode()
            resp_xml = fromstring(resp_bytes)
            for key in resp_xml.findall("{*}Contents/{*}Key"):
                files.append(key.text)
            for prefix in resp_xml.findall("{*}CommonPrefixes/{*}Prefix"):
                directories.append(prefix.text)
            continuationToken = resp_xml.find("{*}NextContinuationToken")
            if continuationToken is not None:
                query["continuation-token"] = continuationToken.text
            else:
                break
        if path:
            files = [f.split(path, 1)[1] for f in files]
            directories = [d.split(path, 1)[1] for d in directories]
        directories = [d.rstrip("/") for d in directories]
        return directories, files

    def size(self, name):
        resp = s3_request("HEAD", f"/{name}", **self.aws_conf)
        assert resp.status == 200
        return int(resp.headers["Content-Length"])

    def url(self, name):
        return f"{settings.MEDIA_URL.rstrip('/')}/{name}"
