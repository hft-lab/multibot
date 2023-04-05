from bravado.requests_client import Authenticator
import hmac
import urllib
import hashlib
import time


class APIKeyAuthenticator(Authenticator):
    """?api_key authenticator.
    This authenticator adds BitMEX API key support via header.
    :param host: Host to authenticate for.
    :param api_key: API key.
    :param api_secret: API secret.
    """

    def __init__(self, host, api_key, api_secret):
        super(APIKeyAuthenticator, self).__init__(host)
        self.api_key = api_key
        self.api_secret = api_secret

    # Forces this to apply to all requests.
    @staticmethod
    def matches(url):
        if "swagger.json" in url:
            return False
        return True

    # Add the proper headers via the `expires` scheme.
    def apply(self, r):
        # 5s grace period in case of clock skew
        expires = int(round(time.time()) + 3600)
        r.headers['api-expires'] = str(expires)
        r.headers['api-key'] = self.api_key
        prepared = r.prepare()
        body = prepared.body or ''
        url = prepared.path_url
        # print(json.dumps(r.data,  separators=(',',':')))
        r.headers['api-signature'] = self.generate_signature(self.api_secret, r.method, url, expires, body)
        # print(f"REQUEST HEADER{r.headers}")
        # print(f"REQ BODY: {body}")
        return r

    @staticmethod
    def generate_nonce():
        return int(round(time.time() + 3600))

    # Generates an API signature.
    # A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
    # Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
    # and the data, if present, must be JSON without whitespace between keys.
    #
    # For example, in psuedocode (and in real code below):
    #
    # verb=POST
    # url=/api/v1/order
    # nonce=1416993995705
    # data={"symbol":"XBTZ14","quantity":1,"price":395.01}
    # signature = HEX(HMAC_SHA256(secret, 'POST/api/v1/order1416993995705{"symbol":"XBTZ14","quantity":1,"price":395.01}'))
    # def generate_signature(self, secret, verb, url, nonce, data):
    #     """Generate a request signature compatible with BitMEX."""
    #     # Parse the url so we can remove the base and extract just the path.
    #     parsedURL = urllib.parse.urlparse(url)
    #     path = parsedURL.path
    #     if parsedURL.query:
    #         path = path + '?' + parsedURL.query
    #
    #     # print "Computing HMAC: %s" % verb + path + str(nonce) + data
    #     message = (verb + path + str(nonce) + data).encode('utf-8')
    #
    #     signature = hmac.new(secret.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()
    #     return signature

    # Generates an API signature.
    # A signature is HMAC_SHA256(secret, verb + path + expires + data), hex encoded.
    # Verb must be uppercased, url is relative, expires must be an increasing 64-bit integer
    # and the data, if present, must be JSON without whitespace between keys.
    #
    # For example, in psuedocode (and in real code below):
    #

    @staticmethod
    def generate_signature(secret, verb, url, nonce, data=''):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        if parsed_url.query:
            path = path + '?' + parsed_url.query
        message = bytes(verb + path + str(nonce) + data, 'utf-8')
        signature = hmac.new(bytes(secret, 'utf-8'), message, digestmod=hashlib.sha256).hexdigest()
        # print(f"Path: {path}\nMethod: {verb}\nMessage: {message}\nSignature: {signature}\n")
        # print("Computing HMAC: %s" % message)
        # print("Signature: %s" % signature)
        return signature



