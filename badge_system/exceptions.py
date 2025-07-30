class HTTPRequestError(Exception):
    def __init__(self, status: int, url: str, reason: str, method: str, body: str = None, ):
        self.status = status
        self.url = url
        self.reason = reason
        self.method= method
        self.body = body
        message = f"HTTP {status} Error for {url}: {reason}"
        if body:
            message += f" | Response: {body}"
        super().__init__(message)
    def log_http_error(self):
        print("❌ HTTP Request Failed:")
        print(f"  ➤ Method : {self.method}")
        print(f"  ➤ URL    : {self.url}")
        print(f"  ➤ Status : {self.status}")
        print(f"  ➤ Reason : {self.reason}")
        if hasattr(self.body, 'body') and self.body:
            print(f"  ➤ Body   : {self.body}")
            

class DBError(Exception):
    def __init__(self, status: int,reason: str, collection:str, message: str):
        super().__init__(message)
        self.message = message
        self.status = status
        self.reason = reason
        self.collection = collection

    def log_db_error(self):
        print("❌ Database Request Failed:")
        print(f"  ➤ Error  : {self.message}")
        print(f"  ➤ Status : {self.status}")
        print(f"  ➤ Reason : {self.reason}")
        print(f"  ➤ Collection : {self.collection}")
        