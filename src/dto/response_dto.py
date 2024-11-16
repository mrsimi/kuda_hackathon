class ResponseDto:
    def __init__(self, isSuccessful:bool, message:str,
                  data:object, status_code:int):
        self.isSuccessful = isSuccessful
        self.message = message
        self.data = data
        self.statuscode = status_code

    def to_dict(self):
        return {
            "isSuccessful": self.isSuccessful,
            "message": self.message,
            "data": self.data, 
            "statuscode": self.statuscode
        }