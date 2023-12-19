import json
import os
import urllib.parse
import requests
from django.conf import settings
import pyotp
import pyperclip
import datetime


class KiteSiteLogin:

    root = 'https://kite.zerodha.com'

    paths = dict(
        login='/api/login',
        twofa='/api/twofa',
    )

    def get_url(self):
        return f'https://kite.zerodha.com/connect/login?v=3&api_key={settings.KITE_API_KEY}'

    def format_cookies(self, cookie_jar):
        results = []
        for i, cookie in enumerate(cookie_jar):
            temp = dict()
            temp['domain'] = 'kite.zerodha.com'
            temp['expirationDate'] = (datetime.datetime.now() + datetime.timedelta(days=2)).timestamp()
            temp['name'] = cookie.name
            temp['path'] = cookie.path
            temp['value'] = cookie.value
            temp['sameSite'] = 'unspecified'
            temp['hostOnly'] = True
            temp['httpOnly'] = False
            temp['secure'] = True
            temp['session'] = True
            temp['storeId'] = "0"
            temp['id'] = i
            results.append(temp)

        print("Use following cookies in chrome session. Its already copied in clipboard")
        print('-----------------')
        x = json.dumps(results)
        print(x)
        pyperclip.copy(x)
        print('-----------------')

    def get_request_token(self):
        headers = dict()
        headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        session = requests.Session()
        session.headers = headers
        url = self.get_url()
        resp1 = session.get(url)
        url1 = resp1.url
        login_data = dict(
            user_id=settings.KITE_USER_ID,
            password=settings.KITE_PASSWORD,
        )
        login = session.post(self.root + self.paths.get('login'), data=login_data)
        data = json.loads(login.text)
        totp = pyotp.TOTP(settings.KITE_TOTP_SECRET)
        twofa_data = dict(
            user_id=settings.KITE_USER_ID,
            twofa_value=totp.now(),
            skip_session='',
            request_id=data.get('data', {}).get('request_id')
        )
        print(twofa_data)
        twofa = session.post(self.root + self.paths.get('twofa'), data=twofa_data)
        resp2 = session.get(url1, verify=False, )
        self.format_cookies(session.cookies)
        print(resp2.url)
        parsed = urllib.parse.urlparse(resp2.url)
        query = urllib.parse.parse_qs(parsed.query)
        print(parsed.query)
        return query['request_token'][0]
