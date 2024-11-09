import io, datetime, hashlib, hmac, http.client, urllib.parse

def s3_request(
    method, path, query=None, headers=None, body=None, bucket=None,
    aws_region=None, aws_key_id=None, aws_secret=None, session_token=None
):
    """
    Make a request to the S3 API.

    :param str method: S3 API request method (e.g. "PUT").
    :param str path: S3 API request path (e.g. "/test.txt").
    :param dict query: (Optional) S3 API request GET query parameters (e.g. {"list-type": "2"}). 
    :param dict headers: (Optional) S3 API request extra headers (e.g. {"Content-Type": "text/plain"}).
    :param dict body: (Optional) S3 API request body. Can be byte string or file-like object.

    :param str bucket: The S3 bucket (e.g. "examplebucket").
    :param str aws_region: The AWS region (e.g. "us-west-2").
    :param str aws_key_id: The AWS Key ID to use.
    :param str aws_secret: The AWS Key ID's secret.
    :param str session_token: If using temporary credentials (e.g. from EC2 metadata), the session token to use.

    :return: The response from AWS S3 API
    :rtype: http.client.HTTPResponse
    """
    # payload size and hash
    data_hash = hashlib.sha256(body if isinstance(body, bytes) else b"")
    data_len = len(body) if isinstance(body, bytes) else 0
    if hasattr(body, "read") and hasattr(body, "seek"):
        body.seek(0)
        while True:
            chunk = body.read(data_hash.block_size)
            if not chunk:
                break
            data_len += len(chunk)
            data_hash.update(chunk)
        body.seek(0)
    payload_hash = data_hash.hexdigest()
    # reference variables
    host = f"{bucket}.s3.{aws_region}.amazonaws.com"
    NOW = datetime.datetime.now(tz=datetime.timezone.utc)
    NOW_DATE = NOW.strftime("%Y%m%d")
    NOW_DT = NOW.strftime("%Y%m%dT%H%M%SZ")
    # canonical headers
    cheaders = [("host", host), ("x-amz-content-sha256", payload_hash), ("x-amz-date", NOW_DT)]
    cheaders += [(k.lower(), v.strip()) for k, v in (headers or {}).items()]
    cheaders.sort(key=lambda i: i[0])
    cheaders_str = "".join(f"{k}:{v}\n" for k, v in cheaders)
    cheader_names = ";".join(k for k, v in cheaders)
    # signature
    query_str = urllib.parse.urlencode(sorted((query or {}).items(), key=lambda i: i[0]))
    canonical_request = f"{method}\n{path}\n{query_str}\n{cheaders_str}\n{cheader_names}\n{payload_hash}"
    req_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    sig_payload = f"AWS4-HMAC-SHA256\n{NOW_DT}\n{NOW_DATE}/{aws_region}/s3/aws4_request\n{req_hash}"
    kSecret = f"AWS4{aws_secret}"
    kDate = hmac.new(kSecret.encode(), NOW_DATE.encode(), hashlib.sha256).digest()
    kRegion = hmac.new(kDate, aws_region.encode(), hashlib.sha256).digest()
    kService = hmac.new(kRegion, b"s3", hashlib.sha256).digest()
    kReqType = hmac.new(kService, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(kReqType, sig_payload.encode(), hashlib.sha256).hexdigest()
    # request param
    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={aws_key_id}/{NOW_DATE}/{aws_region}/s3/aws4_request,"
        f"SignedHeaders={cheader_names},Signature={signature}"
    )
    req_headers = cheaders + [("Content-Length", str(data_len)), ("Authorization", auth_header)]
    if session_token:
        req_headers.append(("X-Amz-Security-Token", session_token))
    # Make the request
    conn = http.client.HTTPSConnection(host)
    conn.request(method, path + f"?{query_str}", body, dict(req_headers))
    resp = conn.getresponse()
    return resp

